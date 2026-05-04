from pyspark.sql.functions import col, to_json

def get_raw_columns():
    """Returns the base columns. Must be called AFTER SparkSession is created."""
    return [
        col("data.event_id"),
        col("data.event_type"),
        col("data.posted_in_subreddit"),
        col("data.author"),
        col("data.url"),
        col("data.title"),
        col("data.content"),
        col("data.timestamp"),
        col("data.has_media"),
        col("data.media_urls"),
        col("data.score"),
        col("data.upvote_ratio"),
        col("data.num_comments"),
        col("data.is_crosspost"),
        col("data.original_subreddit"),
    ]

def get_enriched_columns():
    """Returns the enriched columns. Must be called AFTER SparkSession is created."""
    return get_raw_columns() + [
        col("data.enrichment.languages").alias("languages"),
        col("data.enrichment.translation").alias("translation"),
        col("data.enrichment.sentiment_score").alias("sentiment_score"),
        col("data.enrichment.intent").alias("intent"),
        col("data.enrichment.topics").alias("topics"),
        to_json(col("data.enrichment.entities")).alias("entities")
    ]