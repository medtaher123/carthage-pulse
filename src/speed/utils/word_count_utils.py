from pyspark.sql.connect.functions import regexp_replace
from pyspark.sql.functions import col, explode, split, lower, window, length, concat_ws
from pyspark.sql import DataFrame


STOP_WORDS = [
    "the", "and", "this", "that", "with", "from", "for",
    "what", "your", "would", "want", "have", "when", "they", "there", "which", "about", "like", "just", "more",
    "here", "actually",
    "eli", "mta3", "ala", "fama", "haka", "bech", "fi", # Derja/Arabic
    "ken", "houwa", "hiyya", "mouch", "bara", "aya"
]


def extract_trending_words(df: DataFrame) -> DataFrame:
    """Transforms raw text into windowed word counts."""

    # 1. Combine title and content, and make it lowercase
    combined_text = lower(concat_ws(" ", col("title"), col("content")))

    # 2. CLEANUP: Replace all punctuation with a space
    # (We use a space instead of "" so "hello/world" becomes "hello world" instead of "helloworld")
    cleaned_text = regexp_replace(combined_text, r"\p{Punct}", " ")

    # 1. Explode text into words
    words_df = df.select(
        col("timestamp"),
        explode(
            split(cleaned_text, "\\s+")
        ).alias("word")    )

    # 2. Filter out stop words and junk (FIXED: using length() instead of len())
    filtered_words = words_df.filter(
        (~col("word").isin(STOP_WORDS)) & (length(col("word")) > 3)
    )

    # 3. Aggregate with a 10-minute window, sliding every 5 minutes
    # Using watermarks allows Spark to drop old data from memory safely
    trending_words = (
        filtered_words
        .withWatermark("timestamp", "30 seconds")
        .groupBy(
            window(col("timestamp"), "30 seconds", "15 seconds").alias("time_window"),
            col("word")
        )
        .count()
        .select(
            col("time_window.start").alias("window_start"),
            col("time_window.end").alias("window_end"),
            col("word"),
            col("count")
        )
    )

    return trending_words


# 10 minutes -> 30 seconds
# 5 minutes -> 15 seconds