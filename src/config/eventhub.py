from databricks.sdk.runtime import dbutils

# Event Hubs configuration

CLICK_STREAM_TOPIC_CONN_STR = dbutils.secrets.get("streampulse","CLICK_STREAM_TOPIC_CONN_STR")

PLAYBACK_EVENTS_TOPIC_CONN_STR = dbutils.secrets.get("streampulse","PLAYBACK_EVENTS_TOPIC_CONN_STR")
# Kafka Consumer configuration

CLICK_STREAM_EVENTS_KAFKA_OPTIONS = {
  "kafka.bootstrap.servers"  : "streampulse.servicebus.windows.net:9093",
  "subscribe"                : "click-stream-events",
  "kafka.sasl.mechanism"     : "PLAIN",
  "kafka.security.protocol"  : "SASL_SSL",
  "kafka.sasl.jaas.config"   : f"kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$ConnectionString\" password=\"{CLICK_STREAM_TOPIC_CONN_STR}\";",
  "kafka.request.timeout.ms" :10000,
  "kafka.session.timeout.ms" : 10000,
  "maxOffsetsPerTrigger"     : 10000,
  "failOnDataLoss"           : 'true',
  "startingOffsets"          : "latest"
}

PLAYBACK_EVENTS_KAFKA_OPTIONS = {
  "kafka.bootstrap.servers"  : "streampulse.servicebus.windows.net:9093",
  "subscribe"                : "playback-events",
  "kafka.sasl.mechanism"     : "PLAIN",
  "kafka.security.protocol"  : "SASL_SSL",
  "kafka.sasl.jaas.config"   : f"kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$ConnectionString\" password=\"{PLAYBACK_EVENTS_TOPIC_CONN_STR}\";",
  "kafka.request.timeout.ms" :10000,
  "kafka.session.timeout.ms" : 10000,
  "maxOffsetsPerTrigger"     : 10000,
  "failOnDataLoss"           : 'true',
  "startingOffsets"          : "latest"
}