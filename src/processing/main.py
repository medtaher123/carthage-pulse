"""Main entry point for Reddit processing (enrichment) service"""

import logging
from typing import Optional
from src.processing.consumer import Consumer
from src.shared_utils import setup_logging

logger = setup_logging(logging.INFO)
logging.getLogger("httpx").setLevel(logging.WARNING)


def main(max_runtime: Optional[float] = None):
    """
    Start the enrichment service.

    Args:
        max_runtime: If given, the service will gracefully exit after
                     this many seconds. Airflow uses this to restart the
                     process periodically so it remains manageable.
    """
    logger.info("Initializing Reddit Processing Service")
    consumer = Consumer(max_runtime=max_runtime)
    logger.info("Starting enrichment pipeline")
    try:
        consumer.run()
    except KeyboardInterrupt:
        logger.info("Interrupted - triggering shutdown")
        consumer.shutdown()
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
