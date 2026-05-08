import sys
from pyspark.sql.functions import col, to_date, to_timestamp, explode, avg, count
from src.batch.utils import get_spark_batch_session, setup_timescale_hypertable, write_batch_to_postgres
from src.shared_utils.config import load_config, get_minio_bucket

def main():
    spark = get_spark_batch_session("Batch-Topic-Sentiment")
    config = load_config()
    bucket = get_minio_bucket(config)

    print("Setting up target table 'batch_topic_sentiment'...")
    create_sql = """
    CREATE TABLE IF NOT EXISTS batch_topic_sentiment (
        day_start TIMESTAMP NOT NULL,
        topic VARCHAR NOT NULL,
        avg_sentiment FLOAT,
        sentiment_count BIGINT,
        post_count BIGINT,
        PRIMARY KEY (day_start, topic)
    );
    """
    setup_timescale_hypertable("batch_topic_sentiment", create_sql, "day_start")

    # Read enriched data from minio
    s3_path = f"s3a://{bucket}/batch/"
    print(f"Reading data from {s3_path}...")
    try:
        df = spark.read.parquet(s3_path)
    except Exception as e:
        print(f"Failed to read parquet data from MinIO: {e}")
        sys.exit(1)

    print(f"Deduplicating events by event_id...")
    initial_count = df.count()
    df = df.dropDuplicates(["event_id"])
    final_count = df.count()
    print(f"Removed {initial_count - final_count} duplicate events")

    print("Computing daily sentiment per topic...")
    
    if "enrichment_sentiment" not in df.columns or "enrichment_topics" not in df.columns:
        print("Required columns not found in data. Exiting.")
        sys.exit(1)

    topic_sentiment_df = (
        df.withColumn("day_start", to_timestamp(to_date(col("timestamp"))))
        .withColumn("topic", explode(col("enrichment_topics")))
        .groupBy("day_start", "topic")
        .agg(
            avg("enrichment_sentiment").alias("avg_sentiment"),
            count("enrichment_sentiment").alias("sentiment_count"),
            count("*").alias("post_count")
        )
    )

    print("Writing results to Postgres...")
    write_batch_to_postgres(topic_sentiment_df, "batch_topic_sentiment", mode="overwrite")
    print("Topic sentiment batch job completed successfully.")

if __name__ == "__main__":
    main()
