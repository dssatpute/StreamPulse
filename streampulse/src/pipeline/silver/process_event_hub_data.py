import pyspark.sql.functions as f
from pyspark import pipelines as dp

from utilities.schema import click_stream_schema, playback_events_schema

# =============================================================================
# INTERMEDIATE TABLES: Parse all records (no validation) 
# =============================================================================

@dp.table(name="streampulse.silver._intermediate_click_stream")
def _intermediate_click_stream():
    """Parse clickstream JSON - no validation, captures all records including malformed"""
    raw_df = spark.readStream.table("streampulse.bronze.user_click_stream_events")
    
    return (
        raw_df.select(
            f.from_json(f.col("value").cast("string"), click_stream_schema).alias("data"),
            f.col("topic"),
            f.col("value").alias("original_value"))
        .select("data.*", "topic", "original_value")
        .withColumn("event_processing_timestamp", f.current_timestamp())
    )

@dp.table(name="streampulse.silver._intermediate_playback")
def _intermediate_playback():
    """Parse playback JSON - no validation, captures all records including malformed"""
    raw_df = spark.readStream.table("streampulse.bronze.user_playback_events")
    
    return (
        raw_df.select(
            f.from_json(f.col("value").cast("string"), playback_events_schema).alias("data"),
            f.col("topic"),
            f.col("value").alias("original_value"))
        .select("data.*", "topic", "original_value")
        .withColumn("event_processing_timestamp", f.current_timestamp())
    )

# =============================================================================
# MAIN SILVER TABLES: Clean data with quality expectations
# =============================================================================

# --- Identity / timestamp: drop rows that can't be attributed or placed in time ---
@dp.expect_or_drop("valid_event_id",   "event_id IS NOT NULL")
@dp.expect_or_drop("valid_user_id",    "user_id IS NOT NULL")
@dp.expect_or_drop("valid_session_id", "session_id IS NOT NULL")
@dp.expect_or_drop("valid_timestamp",  "event_timestamp IS NOT NULL")
# --- Enum validation: drop rows with unknown categorical values ---
@dp.expect_all_or_drop({
    "known_event_type": "event_type IN ('page_view', 'search', 'browse', 'click_title', 'add_watchlist', 'remove_watchlist', 'rate', 'share')",
    "known_page":       "page IN ('home', 'search', 'browse_genre', 'title_detail', 'my_list', 'settings')",
    "known_device":     "device_type IN ('smart_tv', 'mobile', 'tablet', 'desktop')",
})
# --- Business logic: warn but keep — partial data is still useful ---
@dp.expect("search_has_query",      "NOT (event_type = 'search' AND search_query IS NULL)")
@dp.expect("content_event_has_id",  "NOT (event_type IN ('click_title', 'add_watchlist', 'remove_watchlist', 'rate') AND content_id IS NULL)")
@dp.table(name="streampulse.silver.user_click_stream_events")
def user_click_stream_events():
    """Clean clickstream events - only valid records pass through"""
    return spark.readStream.table("streampulse.silver._intermediate_click_stream").drop("original_value")


# --- Timestamp: FAIL the pipeline — every Gold window aggregation depends on this ---
@dp.expect_or_fail("valid_event_timestamp", "event_timestamp IS NOT NULL")
# --- Identity + numeric sanity: drop unattributable or physically impossible rows ---
@dp.expect_all_or_drop({
    "valid_event_id":   "event_id IS NOT NULL",
    "valid_user_id":    "user_id IS NOT NULL",
    "valid_session_id": "session_id IS NOT NULL",
    "valid_content_id": "content_id IS NOT NULL",
    "known_event_type": "event_type IN ('play', 'pause', 'resume', 'stop', 'seek', 'heartbeat', 'buffer_start', 'buffer_end', 'error')",
    "valid_position":   "playback_position_sec IS NULL OR playback_position_sec >= 0",
    "valid_buffer":     "buffer_duration_ms IS NULL OR buffer_duration_ms >= 0",
    "valid_latency":    "latency_ms IS NULL OR latency_ms >= 0",
})
# --- Heartbeats must report bitrate — they are the primary QoE sample ---
@dp.expect_or_drop("valid_bitrate_on_heartbeat", "NOT (event_type = 'heartbeat' AND (bitrate_kbps IS NULL OR bitrate_kbps <= 0))")
# --- Unknown resolution: warn only — new values (e.g. 8K) may appear before enum is updated ---
@dp.expect("known_resolution", "resolution IS NULL OR resolution IN ('480p', '720p', '1080p', '4K')")
@dp.table(name="streampulse.silver.user_playback_events")
def user_playback_events():
    """Clean playback events - only valid records pass through"""
    return spark.readStream.table("streampulse.silver._intermediate_playback").drop("original_value")

# =============================================================================
# QUARANTINE TABLES: Capture rejected records for inspection/reprocessing
# =============================================================================

@dp.table(name="streampulse.silver.quarantine_click_stream_events")
def quarantine_click_stream_events():
    """
    Captures clickstream records that failed validation.
    Includes failure reasons and original payload for debugging.
    """
    df = spark.readStream.table("streampulse.silver._intermediate_click_stream")
    
    return (
        df
        # Keep only records that fail at least one validation
        .filter(
            "(event_id IS NULL) OR "
            "(user_id IS NULL) OR "
            "(session_id IS NULL) OR "
            "(event_timestamp IS NULL) OR "
            "(event_type NOT IN ('page_view', 'search', 'browse', 'click_title', 'add_watchlist', 'remove_watchlist', 'rate', 'share')) OR "
            "(page NOT IN ('home', 'search', 'browse_genre', 'title_detail', 'my_list', 'settings')) OR "
            "(device_type NOT IN ('smart_tv', 'mobile', 'tablet', 'desktop'))"
        )
        # Add failure reason
        .withColumn("failure_reasons", f.array_distinct(f.array(
            f.when(f.col("event_id").isNull(), f.lit("missing_event_id")),
            f.when(f.col("user_id").isNull(), f.lit("missing_user_id")),
            f.when(f.col("session_id").isNull(), f.lit("missing_session_id")),
            f.when(f.col("event_timestamp").isNull(), f.lit("missing_timestamp")),
            f.when(~f.col("event_type").isin('page_view', 'search', 'browse', 'click_title', 'add_watchlist', 'remove_watchlist', 'rate', 'share'), 
                   f.lit("unknown_event_type")),
            f.when(~f.col("page").isin('home', 'search', 'browse_genre', 'title_detail', 'my_list', 'settings'), 
                   f.lit("unknown_page")),
            f.when(~f.col("device_type").isin('smart_tv', 'mobile', 'tablet', 'desktop'), 
                   f.lit("unknown_device_type"))
        )))
        .withColumn("quarantined_at", f.current_timestamp())
    )

@dp.table(name="streampulse.silver.quarantine_playback_events")
def quarantine_playback_events():
    """
    Captures playback records that failed validation.
    Includes failure reasons and original payload for debugging.
    Note: Does NOT capture expect_or_fail violations (those stop the pipeline).
    """
    df = spark.readStream.table("streampulse.silver._intermediate_playback")
    
    return (
        df
        # Keep only records that fail at least one drop-level validation
        .filter(
            "(event_id IS NULL) OR "
            "(user_id IS NULL) OR "
            "(session_id IS NULL) OR "
            "(content_id IS NULL) OR "
            "(event_type NOT IN ('play', 'pause', 'resume', 'stop', 'seek', 'heartbeat', 'buffer_start', 'buffer_end', 'error')) OR "
            "(playback_position_sec IS NOT NULL AND playback_position_sec < 0) OR "
            "(buffer_duration_ms IS NOT NULL AND buffer_duration_ms < 0) OR "
            "(latency_ms IS NOT NULL AND latency_ms < 0) OR "
            "(event_type = 'heartbeat' AND (bitrate_kbps IS NULL OR bitrate_kbps <= 0))"
        )
        # Add failure reason
        .withColumn("failure_reasons", f.array_distinct(f.array(
            f.when(f.col("event_id").isNull(), f.lit("missing_event_id")),
            f.when(f.col("user_id").isNull(), f.lit("missing_user_id")),
            f.when(f.col("session_id").isNull(), f.lit("missing_session_id")),
            f.when(f.col("content_id").isNull(), f.lit("missing_content_id")),
            f.when(~f.col("event_type").isin('play', 'pause', 'resume', 'stop', 'seek', 'heartbeat', 'buffer_start', 'buffer_end', 'error'),
                   f.lit("unknown_event_type")),
            f.when((f.col("playback_position_sec").isNotNull()) & (f.col("playback_position_sec") < 0),
                   f.lit("negative_playback_position")),
            f.when((f.col("buffer_duration_ms").isNotNull()) & (f.col("buffer_duration_ms") < 0),
                   f.lit("negative_buffer_duration")),
            f.when((f.col("latency_ms").isNotNull()) & (f.col("latency_ms") < 0),
                   f.lit("negative_latency")),
            f.when((f.col("event_type") == "heartbeat") & ((f.col("bitrate_kbps").isNull()) | (f.col("bitrate_kbps") <= 0)),
                   f.lit("invalid_heartbeat_bitrate"))
        )))
        .withColumn("quarantined_at", f.current_timestamp())
    )
