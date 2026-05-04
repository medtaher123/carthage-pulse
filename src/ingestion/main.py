"""Main entry point for Reddit ingestion service"""

import logging
import time
from src.ingestion.reddit_client import RedditClient
from src.ingestion.producer import KafkaProducer
from src.shared_utils.config import load_config
from src.shared_utils import setup_logging

logger = setup_logging(logging.INFO)


def main():
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
            logger.info(
                f"Initial fetch: {len(events)} items ({media_count} with media)"
            )

            sent = kafka_producer.send_batch(events)
            logger.info(f"Sent {sent}/{len(events)} events to Kafka")
        else:
            logger.info("Initial fetch skipped")

        logger.info("Entering streaming loop...\n")

        while True:
            new_events = client.poll()

            if new_events:
                media_count = sum(1 for e in new_events if e.has_media)
                logger.info(
                    f"Found {len(new_events)} new events ({media_count} with media)!"
                )
                sent = kafka_producer.send_batch(new_events)
                for event in new_events:
                    icon = (
                        "📷"
                        if event.has_media
                        else ("📝" if event.event_type == "POST" else "💬")
                    )
                    display_text = (
                        event.title if event.event_type == "POST" else event.content
                    ) or ""
                    media_info = (
                        f" [{len(event.media_urls)} media]" if event.has_media else ""
                    )
                    logger.info(
                        f"[{icon}] r/{event.posted_in_subreddit} {event.author} ({event.score or 0}↑): {display_text[:50]}...{media_info}"
                    )
            else:
                logger.debug("No new items, sleeping...")

            time.sleep(client.poll_interval)

    except KeyboardInterrupt:
        logger.info("Ingestion service stopped by user.")
    except Exception as e:
        logger.exception(f"Unexpected error in main: {e}")
        raise
    finally:
        # Ensure producer is safely closed
        if kafka_producer is not None:
            try:
                kafka_producer.flush()
                kafka_producer.close()
                logger.info("Kafka producer closed successfully")
            except Exception as e:
                logger.error(f"Error closing Kafka producer: {e}")


if __name__ == "__main__":
    main()
