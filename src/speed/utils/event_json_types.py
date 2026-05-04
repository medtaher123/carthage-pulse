from typing import Any
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_json
from pyspark.sql.types import (
    StructType, StructField, StringType, BooleanType,
    IntegerType, FloatType, ArrayType, TimestampType
)

entity_schema = StructType([
    StructField("name", StringType(), True),
    StructField("type", StringType(), True)
])

enrichment_schema = StructType([
    StructField("languages", ArrayType(StringType()), True),
    StructField("translation", StringType(), True),
    StructField("sentiment_score", FloatType(), True),
    StructField("intent", StringType(), True),
    StructField("topics", ArrayType(StringType()), True),
    StructField("entities", ArrayType(entity_schema), True)
])

enriched_event_json_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("posted_in_subreddit", StringType(), True),
    StructField("author", StringType(), True),
    StructField("url", StringType(), True),
    StructField("title", StringType(), True),
    StructField("content", StringType(), True),
    StructField("timestamp", TimestampType(), True), # Spark will auto-cast ISO-8601 strings
    StructField("has_media", BooleanType(), True),
    StructField("media_urls", ArrayType(StringType()), True),
    StructField("score", IntegerType(), True),
    StructField("upvote_ratio", FloatType(), True),
    StructField("num_comments", IntegerType(), True),
    StructField("is_crosspost", BooleanType(), True),
    StructField("original_subreddit", StringType(), True),
    StructField("enrichment", enrichment_schema, True)
])

raw_event_json_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("posted_in_subreddit", StringType(), True),
    StructField("author", StringType(), True),
    StructField("url", StringType(), True),
    StructField("title", StringType(), True),
    StructField("content", StringType(), True),
    StructField("timestamp", TimestampType(), True), # Spark will auto-cast ISO-8601 strings
    StructField("has_media", BooleanType(), True),
    StructField("media_urls", ArrayType(StringType()), True),
    StructField("score", IntegerType(), True),
    StructField("upvote_ratio", FloatType(), True),
    StructField("num_comments", IntegerType(), True),
    StructField("is_crosspost", BooleanType(), True),
    StructField("original_subreddit", StringType(), True),
])