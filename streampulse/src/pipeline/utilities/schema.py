import pyspark.sql.types as T

click_stream_schema = T.StructType([
    T.StructField("event_id",        T.StringType(),    nullable=True),
    T.StructField("user_id",         T.StringType(),    nullable=True),
    T.StructField("session_id",      T.StringType(),    nullable=True),
    T.StructField("event_type",      T.StringType(),    nullable=True),
    T.StructField("page",            T.StringType(),    nullable=True),
    T.StructField("content_id",      T.StringType(),    nullable=True),  # nullable — can be null
    T.StructField("search_query",    T.StringType(),    nullable=True),  # nullable — only for search events
    T.StructField("event_timestamp", T.TimestampType(), nullable=True),
    T.StructField("device_type",     T.StringType(),    nullable=True),
    T.StructField("device_os",       T.StringType(),    nullable=True),
    T.StructField("browser",         T.StringType(),    nullable=True)
])

playback_events_schema = schema = T.StructType([
    T.StructField("event_id",             T.StringType(),    nullable=False),
    T.StructField("user_id",              T.StringType(),    nullable=False),
    T.StructField("session_id",           T.StringType(),    nullable=False),
    T.StructField("content_id",           T.StringType(),    nullable=False),
    T.StructField("event_type",           T.StringType(),    nullable=False),
    T.StructField("playback_position_sec",T.IntegerType(),   nullable=True),
    T.StructField("bitrate_kbps",         T.IntegerType(),   nullable=True),
    T.StructField("resolution",           T.StringType(),    nullable=True),
    T.StructField("buffer_duration_ms",   T.IntegerType(),   nullable=True),
    T.StructField("latency_ms",           T.IntegerType(),   nullable=True),
    T.StructField("event_timestamp",      T.TimestampType(), nullable=False),
    T.StructField("device_type",          T.StringType(),    nullable=True),
    T.StructField("device_os",            T.StringType(),    nullable=True),
    T.StructField("app_version",          T.StringType(),    nullable=True),
    T.StructField("geo_country",          T.StringType(),    nullable=True),
    T.StructField("geo_region",           T.StringType(),    nullable=True),
    T.StructField("isp",                  T.StringType(),    nullable=True),
])
