import sys
from pyspark.sql.functions import col, to_date, avg, count
from src.batch.utils import get_spark_batch_session, setup_timescale_hypertable, write_batch_to_postgres
from src.shared_utils.config import load_config, get_minio_bucket

def main():
    spark = get_spark_batch_session("Batch-Daily-Sentiment")
    config = load_config()
    bucket = get_minio_bucket(config)

    print("Setting up target table 'batch_daily_sentiment'...")
    create_sql = """
    CREATE TABLE IF NOT EXISTS batch_daily_sentiment (
        day_start TIMESTAMP NOT NULL,
        subreddit VARCHAR NOT NULL,
        avg_sentiment FLOAT,
        sentiment_count BIGINT,
        post_count BIGINT,
        avg_score FLOAT,
        sentiment_stddev FLOAT,
        sentiment_min FLOAT,
        sentiment_max FLOAT,
        sample_event_ids TEXT[],
        PRIMARY KEY (day_start, subreddit)
    );
    """
    setup_timescale_hypertable("batch_daily_sentiment", create_sql, "day_start")

    # Read enriched data from minio
    s3_path = f"s3a://{bucket}/batch/"
    print(f"Reading data from {s3_path}...")
    try:
        df = spark.read.parquet(s3_path)
    except Exception as e:
        print(f"Failed to read parquet data from MinIO: {e}")
        sys.exit(1)

    print("Computing daily sentiment per subreddit...")
    
    # Group by date and subreddit, then compute average sentiment
    if "enrichment_sentiment" not in df.columns:
        print("Column 'enrichment_sentiment' not found in data. Exiting.")
        sys.exit(1)

    from pyspark.sql.functions import expr, slice, to_timestamp

    daily_sentiment_df = (
        df.withColumn("day_start", to_timestamp(to_date(col("timestamp"))))
        .groupBy("day_start", "posted_in_subreddit")
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
    write_batch_to_postgres(daily_sentiment_df, "batch_daily_sentiment", mode="overwrite")
    print("Daily sentiment batch job completed successfully.")

if __name__ == "__main__":
    main()
