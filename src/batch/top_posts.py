import sys
from pyspark.sql.functions import col, to_date, to_timestamp, row_number
from pyspark.sql.window import Window
from src.batch.utils import get_spark_batch_session, setup_timescale_hypertable, write_batch_to_postgres
from src.shared_utils.config import load_config, get_minio_bucket

def main():
    spark = get_spark_batch_session("Batch-Top-Posts")
    config = load_config()
    bucket = get_minio_bucket(config)

    print("Setting up target table 'batch_top_posts'...")
    create_sql = """
    CREATE TABLE IF NOT EXISTS batch_top_posts (
        day_start TIMESTAMP NOT NULL,
        subreddit VARCHAR NOT NULL,
        event_id VARCHAR NOT NULL,
        title TEXT,
        score INTEGER,
        sentiment_score FLOAT,
        PRIMARY KEY (day_start, subreddit, event_id)
    );
    """
    setup_timescale_hypertable("batch_top_posts", create_sql, "day_start")

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

    print("Computing top posts per subreddit per day...")
    
    # Define window to rank posts by score per day and subreddit
    window_spec = Window.partitionBy(to_date(col("timestamp")), col("posted_in_subreddit")).orderBy(col("score").desc())

    top_posts_df = (
        df.withColumn("day_start", to_timestamp(to_date(col("timestamp"))))
        .withColumn("rank", row_number().over(window_spec))
        .filter(col("rank") <= 10)
        .select(
            "day_start",
            col("posted_in_subreddit").alias("subreddit"),
            "event_id",
            "title",
            "score",
            col("enrichment_sentiment").alias("sentiment_score")
        )
    )

    print("Writing results to Postgres...")
    write_batch_to_postgres(top_posts_df, "batch_top_posts", mode="overwrite")
    print("Top posts batch job completed successfully.")

if __name__ == "__main__":
    main()
