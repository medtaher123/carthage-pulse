from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_json
from kafka_utils import SPARK_CONNECT_TARGET, read_from_kafka
from postgres_utils import write_to_postgres
from event_json_types import enriched_event_json_schema
from spark_columns import get_enriched_columns

KAFKA_TOPIC = "reddit-events-enriched"

spark = (
    SparkSession.builder
    .appName(KAFKA_TOPIC)
    .remote(SPARK_CONNECT_TARGET)
    .getOrCreate()
)
print(f"Connected to Spark Connect Server at {SPARK_CONNECT_TARGET}.")

streaming_df = read_from_kafka(
    spark = spark,
    topic = KAFKA_TOPIC,
    json_schema = enriched_event_json_schema,
    select_columns=get_enriched_columns(),
)

query = write_to_postgres(
    df=streaming_df,
    checkpoint_id="to_enriched_events",
    db_table="enriched_events"
)

# 3. Await termination to keep the script running
try:
    print(f"Streaming pipeline for {KAFKA_TOPIC} has started.")
    query.awaitTermination()
except KeyboardInterrupt:
    print(f"Stopping stream for topic {KAFKA_TOPIC}...")
    query.stop()