import pyspark.sql.functions as f
from pyspark import pipelines as dp

from utilities.schema import click_stream_schema, playback_events_schema

@dp.table(name = "streampulse.silver.user_click_stream_events")

def user_click_stream_events():

    raw_df = spark.read.table("streampulse.bronze.user_click_stream_events")

    return (
        raw_df.select(
            f.from_json(f.col("value").cast("string"), click_stream_schema).alias("data"),
            f.col("topic").alias("topic"))
        ).select("data.*", "topic").withColumn("event_processing_timestamp", f.current_timestamp())

@dp.table(name = "streampulse.silver.user_playback_events")

def user_playback_events():

    raw_df = spark.read.table("streampulse.bronze.user_playback_events")

    return (
        raw_df.select(
            f.from_json(f.col("value").cast("string"), playback_events_schema).alias("data"),
            f.col("topic").alias("topic"))
        ).select("data.*", "topic").withColumn("event_processing_timestamp", f.current_timestamp())
    
def user_playback_events():

    raw_df = spark.read.table("streampulse.bronze.user_playback_events")

    return (
        raw_df.select(
            f.from_json(f.col("value").cast("string"), playback_events_schema).alias("data"),
            f.col("topic").alias("topic"))
        ).select("data.*", "topic").withColumn("event_processing_timestamp", f.current_timestamp())
