import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark import pipelines as dp

spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()

TRENDING_WINDOW_DURATION = "5 minutes"
TRENDING_WATERMARK_DELAY = "15 minutes"
ENGAGEMENT_SESSION_GAP_DURATION = "30 minutes"
ENGAGEMENT_WATERMARK_DELAY = "2 hours"


@dp.table(
    name="streampulse.gold.trending_title_viewers_5m",
    comment=(
        "Streaming aggregate of unique viewers by content in 5-minute windows. "
        "Uses event-time watermarking to bound state and handle late arrivals."
    ),
    table_properties={
        "quality": "gold",
        "delta.autoOptimize.optimizeWrite": "true",
    },
)
def trending_title_viewers_5m():
    playback_events = (
        spark.readStream.table("streampulse.silver.user_playback_events")
        .withWatermark("event_timestamp", TRENDING_WATERMARK_DELAY)
        .dropDuplicates(["event_id"])
        .filter(f.col("event_type").isin("play", "heartbeat"))
    )

    return (
        playback_events
        .groupBy(
            f.window("event_timestamp", TRENDING_WINDOW_DURATION).alias("event_window"),
            f.col("content_id"),
        )
        .agg(f.count_distinct("user_id").alias("unique_viewers"))
        .select(
            f.col("event_window.start").alias("window_start"),
            f.col("event_window.end").alias("window_end"),
            "content_id",
            "unique_viewers",
        )
        .withColumn("computed_at", f.current_timestamp())
    )

@dp.table(
    name="streampulse.gold.top_trending_titles",
    comment=(
        "Top 10 titles by unique viewers in the last 5 minutes. "
        "Recomputed on every pipeline trigger. "
        "Enriched with content metadata for display. "
        "Powers the 'Top Trending Titles' leaderboard dashboard panel."
    ),
    table_properties={
        "quality": "gold",
        "delta.autoOptimize.optimizeWrite": "true",
    },
)
def top_trending_titles():
    viewer_counts = spark.read.table("streampulse.gold.trending_title_viewers_5m")

    latest_window = viewer_counts.agg(f.max("window_end").alias("latest_window_end"))

    latest_viewer_counts = (
        viewer_counts
        .join(
            latest_window,
            viewer_counts.window_end == latest_window.latest_window_end,
            "inner",
        )
        .drop("latest_window_end")
    )

    content_dim = (
        spark.read.table("streampulse.silver.content_catalog_dim")
        .filter(f.col("__END_AT").isNull())  # current SCD2 snapshot
        .select("content_id", "title", "genre", "content_type", "release_year", "maturity_rating")
    )

    # Window function to rank by viewer count — requires full batch pass
    rank_window = Window.orderBy(f.col("unique_viewers").desc())

    return (
        latest_viewer_counts
        .join(content_dim, "content_id", "left")
        .withColumn("rank", f.rank().over(rank_window))
        .withColumn("content_name", f.col("title"))
        .filter(f.col("rank") <= 10)
        .withColumn("computed_at", f.current_timestamp())
        .select(
            "rank",
            "window_start",
            "window_end",
            "content_id",
            "content_name",
            "title",
            "genre",
            "content_type",
            "release_year",
            "maturity_rating",
            "unique_viewers",
            "computed_at",
        )
    )


@dp.table(
    name="streampulse.gold.content_session_stats_v2",
    comment=(
        "Streaming per-session playback aggregates using event-time session windows. "
        "Watermarking bounds state and limits late-arrival impact."
    ),
    table_properties={
        "quality": "gold",
        "delta.autoOptimize.optimizeWrite": "true",
    },
)
def content_session_stats_v2():
    playback_events = (
        spark.readStream.table("streampulse.silver.user_playback_events")
        .withWatermark("event_timestamp", ENGAGEMENT_WATERMARK_DELAY)
        .dropDuplicates(["event_id"])
    )

    content_dim = (
        spark.read.table("streampulse.silver.content_catalog_dim")
        .filter(f.col("__END_AT").isNull())  # current SCD2 snapshot
        .selectExpr("content_id", "title as content_name"))

    return (
        playback_events
        # Explicitly select columns needed for aggregation to avoid column resolution issues
        .select(
            "event_timestamp",
            "content_id",
            "session_id",
            "user_id",
            "playback_position_sec",
            "event_type"
        )
        .groupBy(
            f.session_window("event_timestamp", ENGAGEMENT_SESSION_GAP_DURATION).alias("event_session"),
            f.col("content_id"),
            f.col("session_id"),
            f.col("user_id"),
        )
        .agg(
            f.max("playback_position_sec").alias("max_watch_depth_sec"),
            f.min("event_timestamp").alias("session_start"),
            f.max("event_timestamp").alias("session_end"),
            f.count(f.when(f.col("event_type") == "buffer_start", 1)).alias("buffer_event_count"),
            f.count(f.when(f.col("event_type") == "error", 1)).alias("error_event_count"),
        )
        .join(content_dim, "content_id", "left")
        .select(
            f.col("event_session.start").alias("event_session_start"),
            f.col("event_session.end").alias("event_session_end"),
            "content_id",
            'content_name',
            "session_id",
            "user_id",
            "max_watch_depth_sec",
            "session_start",
            "session_end",
            "buffer_event_count",
            "error_event_count",
        )
        .withColumn("computed_at", f.current_timestamp())
    )


@dp.table(
    name="streampulse.gold.content_engagement",
    comment=(
        "Per-session watch depth and completion rate per title. "
        "Completion rate = max playback position / total content duration. "
        "Buffer and error event counts included for quality correlation. "
        "Recomputed on every pipeline trigger."
    ),
    table_properties={
        "quality": "gold",
        "delta.autoOptimize.optimizeWrite": "true",
    },
)
def content_engagement():
    session_stats = spark.read.table("streampulse.gold.content_session_stats")

    content_dim = (
        spark.read.table("streampulse.silver.content_catalog_dim")
        .filter(f.col("__END_AT").isNull())
        .select(
            "content_id",
            "title",
            "genre",
            "content_type",
            f.col("duration_min").cast("double").alias("duration_min"),
        )
    )

    return (
        session_stats
        .join(content_dim, "content_id", "left")
        .withColumn("content_name", f.col("title"))
        # completion_rate = watch depth / total duration, capped at 1.0
        .withColumn(
            "completion_rate",
            f.when(
                f.col("duration_min").isNotNull() & (f.col("duration_min") > 0),
                f.least(
                    f.lit(1.0),
                    f.col("max_watch_depth_sec") / (f.col("duration_min") * 60.0),
                ),
            ).otherwise(f.lit(None).cast("double")),
        )
        .withColumn(
            "session_duration_sec",
            f.unix_timestamp("session_end") - f.unix_timestamp("session_start"),
        )
        .select(
            "event_session_start",
            "event_session_end",
            "content_id",
            "content_name",
            "session_id",
            "user_id",
            "title",
            "genre",
            "content_type",
            "max_watch_depth_sec",
            "completion_rate",
            "session_start",
            "session_end",
            "session_duration_sec",
            "buffer_event_count",
            "error_event_count",
        )
    )


@dp.table(
    name="streampulse.gold.subscription_churn_signals",
    comment=(
        "Daily batch: users with declining 7-day engagement cross-referenced with "
        "their current subscription status. Flags churn risk as HIGH / MEDIUM / LOW. "
        "Uses the SCD2 user_profile_dim current snapshot (__END_AT IS NULL). "
        "Recomputed on every pipeline trigger."
    ),
    table_properties={
        "quality": "gold",
        "delta.autoOptimize.optimizeWrite": "true",
    },
)
def subscription_churn_signals():
    seven_days_ago = f.current_timestamp() - f.expr("INTERVAL 7 DAYS")

    # Engagement signals over the past 7 days
    engagement = (
        spark.read.table("streampulse.silver.user_playback_events")
        .filter(f.col("event_timestamp") >= seven_days_ago)
        .groupBy("user_id")
        .agg(
            f.count_distinct("session_id").alias("weekly_sessions"),
            f.count_distinct("content_id").alias("unique_titles_watched"),
            f.avg("playback_position_sec").alias("avg_watch_depth_sec"),
            f.count(f.when(f.col("event_type") == "heartbeat", 1)).alias("total_heartbeats"),
        )
    )

    # Current snapshot only from SCD2: __END_AT IS NULL = active/latest row
    profiles = (
        spark.read.table("streampulse.silver.user_profile_dim")
        .filter(f.col("__END_AT").isNull())
        .select(
            "user_id",
            "username",
            "subscription_plan",
            "subscription_status",
            "country",
            "preferred_language",
        )
    )

    return (
        profiles
        .join(engagement, "user_id", "left")
        # Users with no activity in 7 days get 0 counts, not nulls
        .fillna({"weekly_sessions": 0, "unique_titles_watched": 0, "total_heartbeats": 0})
        .withColumn(
            "churn_risk",
            f.when(
                f.col("subscription_status").isin("cancelled", "paused")
                | (f.col("weekly_sessions") == 0),
                f.lit("HIGH"),
            )
            .when(
                f.col("subscription_status").isin("expired")
                | (f.col("weekly_sessions") <= 2),
                f.lit("MEDIUM"),
            )
            .otherwise(f.lit("LOW")),
        )
        .withColumn("computed_date", f.current_date())
        .select(
            "user_id",
            "username",
            "subscription_plan",
            "subscription_status",
            "country",
            "preferred_language",
            "weekly_sessions",
            "unique_titles_watched",
            "avg_watch_depth_sec",
            "total_heartbeats",
            "churn_risk",
            "computed_date",
        )
        .orderBy("churn_risk", f.col("weekly_sessions").asc())
    )
