from pyspark.sql.connect.functions import lower
from pyspark.sql.functions import col, explode, window, length, trim
from pyspark.sql import DataFrame

def extract_trending_topics(df: DataFrame) -> DataFrame:
    """Transforms events with a 'topics' array into windowed topic counts."""

    # 1. Explode the topics array into individual rows
    topics_df = df.select(
        col("timestamp"),
        explode(col("topics")).alias("topic")
    )

    # 2. Filter out any blank or null topics just to be safe
    cleaned_topics = topics_df.withColumn(
        "topic", lower(trim(col("topic")))
    ).filter(
        col("topic").isNotNull() & (col("topic") != "")
    )

    # 3. Aggregate with a 30-second window, sliding every 15 seconds
    hot_topics = (
        cleaned_topics
        .withWatermark("timestamp", "30 seconds")
        .groupBy(
            window(col("timestamp"), "30 seconds", "15 seconds").alias("time_window"),
            col("topic")
        )
        .count()
        .select(
            col("time_window.start").alias("window_start"),
            col("time_window.end").alias("window_end"),
            col("topic"),
            col("count")
        )
    )

    return hot_topics