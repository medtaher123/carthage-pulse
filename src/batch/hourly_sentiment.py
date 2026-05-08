import sys
from pyspark.sql.functions import col, date_trunc, avg, count, expr, slice
from src.batch.utils import get_spark_batch_session, setup_timescale_hypertable, write_batch_to_postgres
from src.shared_utils.config import load_config, get_minio_bucket

def main():
    spark = get_spark_batch_session("Batch-Hourly-Sentiment")
    config = load_config()
    bucket = get_minio_bucket(config)

    print("Setting up target table 'batch_hourly_sentiment'...")
    create_sql = """
    CREATE TABLE IF NOT EXISTS batch_hourly_sentiment (
        hour_start TIMESTAMP NOT NULL,
        subreddit VARCHAR NOT NULL,
        avg_sentiment FLOAT,
        sentiment_count BIGINT,
        post_count BIGINT,
        avg_score FLOAT,
        sentiment_stddev FLOAT,
        sentiment_min FLOAT,
        sentiment_max FLOAT,
        sample_event_ids TEXT[],
        PRIMARY KEY (hour_start, subreddit)
    );
    """
    setup_timescale_hypertable("batch_hourly_sentiment", create_sql, "hour_start")

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

    print("Computing hourly sentiment per subreddit...")
    
    if "enrichment_sentiment" not in df.columns:
        print("Column 'enrichment_sentiment' not found in data. Exiting.")
        sys.exit(1)

    hourly_sentiment_df = (
        df.withColumn("hour_start", date_trunc("hour", col("timestamp")))
        .groupBy("hour_start", "posted_in_subreddit")
        .agg(
            avg("enrichment_sentiment").alias("avg_sentiment"),
            count("enrichment_sentiment").alias("sentiment_count"),
            count("*").alias("post_count"),
            avg("score").alias("avg_score"),
            expr("stddev_pop(enrichment_sentiment)").alias("sentiment_stddev"),
            expr("min(enrichment_sentiment)").alias("sentiment_min"),
            expr("max(enrichment_sentiment)").alias("sentiment_max"),
            slice(expr("collect_list(event_id)"), 1, 10).alias("sample_event_ids")
        )
        .withColumnRenamed("posted_in_subreddit", "subreddit")
    )

    print("Writing results to Postgres...")
    write_batch_to_postgres(hourly_sentiment_df, "batch_hourly_sentiment", mode="overwrite")
    print("Hourly sentiment batch job completed successfully.")

if __name__ == "__main__":
    main()
