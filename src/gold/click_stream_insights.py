import pyspark.sql.functions as f
from pyspark import pipelines as dp


@dp.table(
    name="streampulse.gold.clickstream_page_engagement_daily",
    comment=(
        "Daily page-level clickstream engagement by page and device type, including "
        "event volume, unique users, and key interaction counts."
    ),
    table_properties={
        "quality": "gold",
        "delta.autoOptimize.optimizeWrite": "true",
    },
)
def clickstream_page_engagement_daily():
    clickstream_events = dp.read("streampulse.silver.user_click_stream_events")

    return (
        clickstream_events
        .withColumn("event_date", f.to_date("event_timestamp"))
        .groupBy("event_date", "page", "device_type")
        .agg(
            f.count("*").alias("total_events"),
            f.count_distinct("user_id").alias("unique_users"),
            f.count_distinct("session_id").alias("unique_sessions"),
            f.sum(f.when(f.col("event_type") == "page_view", 1).otherwise(0)).alias("page_view_events"),
            f.sum(f.when(f.col("event_type") == "search", 1).otherwise(0)).alias("search_events"),
            f.sum(f.when(f.col("event_type") == "click_title", 1).otherwise(0)).alias("title_click_events"),
            f.sum(f.when(f.col("event_type") == "add_watchlist", 1).otherwise(0)).alias("watchlist_add_events"),
        )
        .withColumn("computed_at", f.current_timestamp())
    )


@dp.table(
    name="streampulse.gold.clickstream_content_intent_daily",
    comment=(
        "Daily content-level intent signals from clickstream interactions such as title "
        "clicks, watchlist adds/removes, shares, and ratings."
    ),
    table_properties={
        "quality": "gold",
        "delta.autoOptimize.optimizeWrite": "true",
    },
)
def clickstream_content_intent_daily():
    clickstream_events = (
        dp.read("streampulse.silver.user_click_stream_events")
        .filter(f.col("content_id").isNotNull())
        .withColumn("event_date", f.to_date("event_timestamp"))
    )

    content_dim = (
        dp.read("streampulse.silver.content_catalog_dim")
        .filter(f.col("__END_AT").isNull())
        .select("content_id", "title", "genre", "content_type", "release_year", "maturity_rating")
    )

    content_intent = (
        clickstream_events
        .filter(f.col("event_type").isin("click_title", "add_watchlist", "remove_watchlist", "rate", "share"))
        .groupBy("event_date", "content_id")
        .agg(
            f.count_distinct("user_id").alias("engaged_users"),
            f.count_distinct("session_id").alias("engaged_sessions"),
            f.sum(f.when(f.col("event_type") == "click_title", 1).otherwise(0)).alias("title_click_events"),
            f.sum(f.when(f.col("event_type") == "add_watchlist", 1).otherwise(0)).alias("watchlist_add_events"),
            f.sum(f.when(f.col("event_type") == "remove_watchlist", 1).otherwise(0)).alias("watchlist_remove_events"),
            f.sum(f.when(f.col("event_type") == "share", 1).otherwise(0)).alias("share_events"),
            f.sum(f.when(f.col("event_type") == "rate", 1).otherwise(0)).alias("rating_events"),
        )
        .withColumn(
            "net_watchlist_adds",
            f.col("watchlist_add_events") - f.col("watchlist_remove_events"),
        )
    )

    return (
        content_intent
        .join(content_dim, "content_id", "left")
        .withColumn("computed_at", f.current_timestamp())
        .select(
            "event_date",
            "content_id",
            "title",
            "genre",
            "content_type",
            "release_year",
            "maturity_rating",
            "engaged_users",
            "engaged_sessions",
            "title_click_events",
            "watchlist_add_events",
            "watchlist_remove_events",
            "net_watchlist_adds",
            "share_events",
            "rating_events",
            "computed_at",
        )
    )


@dp.table(
    name="streampulse.gold.clickstream_search_funnel_daily",
    comment=(
        "Daily session-level search funnel showing how many sessions that performed "
        "search also clicked a title, including conversion rate by device type."
    ),
    table_properties={
        "quality": "gold",
        "delta.autoOptimize.optimizeWrite": "true",
    },
)
def clickstream_search_funnel_daily():
    clickstream_events = dp.read("streampulse.silver.user_click_stream_events")

    session_flags = (
        clickstream_events
        .withColumn("event_date", f.to_date("event_timestamp"))
        .groupBy("event_date", "session_id", "device_type")
        .agg(
            f.max(f.when(f.col("event_type") == "search", 1).otherwise(0)).alias("has_search"),
            f.max(f.when(f.col("event_type") == "click_title", 1).otherwise(0)).alias("has_title_click"),
            f.max(f.when(f.col("event_type") == "add_watchlist", 1).otherwise(0)).alias("has_watchlist_add"),
        )
    )

    return (
        session_flags
        .groupBy("event_date", "device_type")
        .agg(
            f.sum("has_search").alias("search_sessions"),
            f.sum(
                f.when((f.col("has_search") == 1) & (f.col("has_title_click") == 1), 1).otherwise(0)
            ).alias("search_to_click_sessions"),
            f.sum(
                f.when((f.col("has_search") == 1) & (f.col("has_watchlist_add") == 1), 1).otherwise(0)
            ).alias("search_to_watchlist_sessions"),
        )
        .withColumn(
            "search_to_click_rate",
            f.when(
                f.col("search_sessions") > 0,
                f.round(f.col("search_to_click_sessions") / f.col("search_sessions"), 4),
            ).otherwise(f.lit(0.0)),
        )
        .withColumn(
            "search_to_watchlist_rate",
            f.when(
                f.col("search_sessions") > 0,
                f.round(f.col("search_to_watchlist_sessions") / f.col("search_sessions"), 4),
            ).otherwise(f.lit(0.0)),
        )
        .withColumn("computed_at", f.current_timestamp())
    )
