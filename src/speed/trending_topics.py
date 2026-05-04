from pyspark.sql import SparkSession
from .utils.kafka_utils import SPARK_CONNECT_TARGET, read_from_kafka, ENRICHED_EVENTS_KAFKA_TOPIC
from .utils.postgres_utils import write_to_postgres
from .utils.event_json_types import raw_event_json_schema, enriched_event_json_schema
from .utils.spark_columns import get_raw_columns, get_enriched_columns
from .utils.trending_topics_utils import extract_trending_topics  # Import the new transformer

def main():
    # 1. Initialize Spark
    spark = (
        SparkSession.builder
        .appName(f"{ENRICHED_EVENTS_KAFKA_TOPIC}-trending-topics-analytics")
        .remote(SPARK_CONNECT_TARGET)
        .getOrCreate()
    )
    print(f"Connected to Spark Connect. Starting trending topics analytics...")

    # 2. Read from Kafka
    raw_df = read_from_kafka(
        spark=spark,
        topic=ENRICHED_EVENTS_KAFKA_TOPIC,
        json_schema=enriched_event_json_schema,
        select_columns=get_enriched_columns()
    )

    # 3. Transform the data into windowed topic counts
    trending_topics_df = extract_trending_topics(raw_df)

    # 4. Write to Postgres
    # Make sure your target table is created in Postgres with a UNIQUE constraint
    # on (window_start, window_end, topic)
    query = write_to_postgres(
        df=trending_topics_df,
        checkpoint_id="trending_topics_job",  # Unique checkpoint for this pipeline!
        db_table="trending_topics_metrics",   # New destination table
        conflict_columns=["window_start", "window_end", "topic"] # Conflict on topic instead of word
    )

    # 5. Await termination
    try:
        print(f"Trending topics pipeline has started.")
        query.awaitTermination()
    except KeyboardInterrupt:
        print(f"Stopping trending topics pipeline...")
        query.stop()