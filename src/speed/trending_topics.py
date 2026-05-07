from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, date_trunc, count
from .utils.kafka_utils import SPARK_CONNECT_TARGET, read_from_kafka, ENRICHED_EVENTS_KAFKA_TOPIC
from .utils.postgres_utils import write_to_postgres
from .utils.event_json_types import enriched_event_json_schema
from .utils.spark_columns import get_enriched_columns
from .utils.trending_topics_utils import extract_trending_topics

def main(max_runtime: int | None = None):
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
    query = write_to_postgres(
        df=trending_topics_df,
        checkpoint_id="trending_topics_job",
        db_table="trending_topics_metrics",
        conflict_columns=["window_start", "window_end", "topic"]
    )

    # 5. Await termination – stop after max_runtime (ms) so Airflow can restart us.
    timeout_ms = max_runtime * 1000 if max_runtime else None
    try:
        print(f"Trending topics pipeline has started.")
        if timeout_ms:
            query.awaitTermination(timeout_ms)
            print("Max runtime reached, cleanly stopping trending topics stream.")
            query.stop()
        else:
            query.awaitTermination()
    except KeyboardInterrupt:
        print(f"Stopping trending topics pipeline...")
        query.stop()
