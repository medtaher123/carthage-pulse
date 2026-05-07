"""Main entry point for Reddit ingestion service"""

import logging
import time
from typing import Optional
from src.ingestion.reddit_client import RedditClient
from src.ingestion.producer import KafkaProducer
from src.shared_utils.config import load_config
from src.shared_utils import setup_logging
from src.shared_utils.service_runner import ServiceRunner

logger = setup_logging(logging.INFO)


def main(max_runtime: Optional[float] = None):
    """Start the ingestion service.

    Args:
        max_runtime: If given, the service will gracefully exit after
                     approximately this many seconds.  When run from Airflow
                     the DAG passes a value a little shorter than the schedule
                     interval so Airflow can restart the process cleanly.
    """
    logger.info("Initializing Reddit Ingestion Service")

    kafka_producer = None
    client = None

    try:
        config = load_config()

        client = RedditClient(config=config)
        kafka_producer = KafkaProducer(config)

        subs = "+".join(client.subreddits)
        logger.info(f"Streaming r/{subs} → Kafka topic: {kafka_producer.topic}")

        if client.do_initial_fetch:
            events = client.initial_fetch()
            media_count = sum(1 for e in events if e.has_media)
            logger.info(f"Initial fetch: {len(events)} items ({media_count} with media)")

            sent = kafka_producer.send_batch(events)
            logger.info(f"Sent {sent}/{len(events)} events to Kafka")
        else:
            logger.info("Initial fetch skipped")

        #  Airflow-friendly runner
        runner = ServiceRunner(
            poll_interval=client.poll_interval,
            max_runtime=max_runtime,
        )

        logger.info("Entering streaming loop…")

        def _poll_once():
            new_events = client.poll()

            if new_events:
                media_count = sum(1 for e in new_events if e.has_media)
                logger.info(f"Found {len(new_events)} new events ({media_count} with media)")
                sent = kafka_producer.send_batch(new_events)
                logger.info(f"Batch delivery: {sent}/{len(new_events)}")
            else:
                logger.debug("No new items, sleeping…")

        runner.run(_poll_once)

    except KeyboardInterrupt:
        logger.info("Ingestion service stopped by user.")
    except Exception as e:
        logger.exception(f"Unexpected error in main: {e}")
        raise
    finally:
        # Ensure producer is safely closed on every exit path
        if kafka_producer is not None:
            try:
                kafka_producer.flush()
                kafka_producer.close()
                logger.info("Kafka producer closed successfully")
            except Exception as exc:
                logger.error(f"Error closing Kafka producer: {exc}")


if __name__ == "__main__":
    main()
