"""Batch layer utilities and job entrypoints.

Expose helper functions for creating Spark sessions and writing
aggregations to Postgres, plus the main job entrypoints so callers
can import jobs from `src.batch`.
"""

from .utils import get_spark_batch_session, setup_timescale_hypertable, write_batch_to_postgres
from .daily_sentiment import main as daily_sentiment_main
from .weekly_topics import main as weekly_topics_main

__all__ = [
	"get_spark_batch_session",
	"setup_timescale_hypertable",
	"write_batch_to_postgres",
	"daily_sentiment_main",
	"weekly_topics_main",
]
