from pyspark.sql.types import *
import pyspark.sql.functions as f
from pyspark import pipelines as dp
from config.eventhub import CLICK_STREAM_EVENTS_KAFKA_OPTIONS, PLAYBACK_EVENTS_KAFKA_OPTIONS

@dp.table(name = "streampulse.bronze.user_click_stream_events")
def user_click_stream_events():
    return (
        spark.readStream
        .format("kafka")
        .options(**CLICK_STREAM_EVENTS_KAFKA_OPTIONS)
        .load()
    )

@dp.table(name = "streampulse.bronze.user_playback_events")
def user_playback_events():
    return (
        spark.readStream
        .format("kafka")
        .options(**PLAYBACK_EVENTS_KAFKA_OPTIONS)
        .load()
    )