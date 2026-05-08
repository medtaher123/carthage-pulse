import sys
from pyspark.sql.functions import col, explode, date_trunc, count
from src.batch.utils import get_spark_batch_session, setup_timescale_hypertable, write_batch_to_postgres
from src.shared_utils.config import load_config, get_minio_bucket

def main():
    spark = get_spark_batch_session("Batch-Weekly-Topics")
    config = load_config()
    bucket = get_minio_bucket(config)

    print("Setting up target table 'batch_weekly_Topics'...")
    create_sql = """
    CREATE TABLE IF NOT EXISTS batch_weekly_topics (
        week_start TIMESTAMP NOT NULL,
        topic VARCHAR NOT NULL,
        topic_count BIGINT,
        PRIMARY KEY (week_start, topic)
    );
    """
    setup_timescale_hypertable("batch_weekly_topics", create_sql, "week_start")

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

    print("Computing weekly topics...")
    
    if "enrichment_topics" not in df.columns:
        print("Column 'enrichment_topics' not found in data. Exiting.")
        sys.exit(1)

    # Group by week start and topic
    weekly_topics_df = (
        df.withColumn("week_start", date_trunc("week", col("timestamp")))
        .withColumn("topic", explode(col("enrichment_topics")))
        .groupBy("week_start", "topic")
        .agg(count("*").alias("topic_count"))
    )

    print("Writing results to Postgres...")
    write_batch_to_postgres(weekly_topics_df, "batch_weekly_topics", mode="overwrite")
    print("Weekly topics batch job completed successfully.")

if __name__ == "__main__":
    main()
