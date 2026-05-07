"""Airflow entrypoint for all Carthage Pulse services.

Usage:
    python airflow/run_task.py <TASK_NAME> [--max-runtime SECONDS]

Tasks:
    ingestion, processing, storage
    speed_save_raw, speed_save_enriched, speed_trending_topics, speed_trending_words
    batch_daily_sentiment, batch_hourly_sentiment, batch_topic_sentiment,
    batch_top_posts, batch_weekly_topics
"""

import argparse
import os
import sys
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Debug: Print environment variables
print(f"DEBUG: KAFKA_BOOTSTRAP_SERVERS={os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'NOT_SET')}")
print(f"DEBUG: MINIO_ENDPOINT={os.getenv('MINIO_ENDPOINT', 'NOT_SET')}")
print(f"DEBUG: POSTGRES_HOST={os.getenv('POSTGRES_HOST', 'NOT_SET')}")
print(f"DEBUG: POSTGRES_PORT={os.getenv('POSTGRES_PORT', 'NOT_SET')}")

# Debug: Print config
from src.shared_utils.config import load_config
config = load_config()
print(f"DEBUG: Config kafka.bootstrap_servers={config.get('kafka', {}).get('bootstrap_servers', 'NOT_FOUND')}")


def main():
    parser = argparse.ArgumentParser(description="Run a Carthage Pulse service for Airflow")
    parser.add_argument("task", help="Name of the task to run")
    parser.add_argument("--max-runtime", type=int, default=None, help="Max runtime in seconds (for long-running services)")
    args = parser.parse_args()

    task = args.task
    max_runtime = args.max_runtime

    # Ingestion
    if task == "ingestion":
        from src.ingestion.main import main as _exec
        _exec(max_runtime=max_runtime)

    # Processing
    elif task == "processing":
        from src.processing.main import main as _exec
        _exec(max_runtime=max_runtime)

    # Storage
    elif task == "storage":
        from src.storage.main import main as _exec
        _exec(max_runtime=max_runtime)

    # Speed Layer
    elif task == "speed_save_raw":
        from src.speed.save_raw import main as _exec
        _exec(max_runtime=max_runtime)
    elif task == "speed_save_enriched":
        from src.speed.save_enriched import main as _exec
        _exec(max_runtime=max_runtime)
    elif task == "speed_trending_topics":
        from src.speed.trending_topics import main as _exec
        _exec(max_runtime=max_runtime)
    elif task == "speed_trending_words":
        from src.speed.trending_words import main as _exec
        _exec(max_runtime=max_runtime)

    # Batch Layer
    elif task == "batch_daily_sentiment":
        from src.batch.daily_sentiment import main as _exec
        _exec()
    elif task == "batch_hourly_sentiment":
        from src.batch.hourly_sentiment import main as _exec
        _exec()
    elif task == "batch_topic_sentiment":
        from src.batch.topic_sentiment import main as _exec
        _exec()
    elif task == "batch_top_posts":
        from src.batch.top_posts import main as _exec
        _exec()
    elif task == "batch_weekly_topics":
        from src.batch.weekly_topics import main as _exec
        _exec()

    else:
        print(f"[ERROR] Unknown task: {task}")
        print("Run with --help for available tasks.\n")
        sys.exit(1)


if __name__ == "__main__":
    main()
