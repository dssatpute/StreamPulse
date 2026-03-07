import pyspark.sql.functions as f
from pyspark import pipelines as dp

from utilities.schema import click_stream_schema, playback_events_schema

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
    raw_df = dp.read_stream("streampulse.bronze.user_click_stream_events")

    return (
        raw_df.select(
            f.from_json(f.col("value").cast("string"), click_stream_schema).alias("data"),
            f.col("topic"))
        .select("data.*", "topic")
        .withColumn("event_processing_timestamp", f.current_timestamp())
    )


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
    raw_df = dp.read_stream("streampulse.bronze.user_playback_events")

    return (
        raw_df.select(
            f.from_json(f.col("value").cast("string"), playback_events_schema).alias("data"),
            f.col("topic"))
        .select("data.*", "topic")
        .withColumn("event_processing_timestamp", f.current_timestamp())
    )
