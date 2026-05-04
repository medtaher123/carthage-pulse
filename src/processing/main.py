"""Main entry point for Reddit processing (enrichment) service"""

import logging
from src.processing.consumer import Consumer
from src.shared_utils import setup_logging

logger = setup_logging(logging.INFO)
logging.getLogger("httpx").setLevel(logging.WARNING)


def main():
    logger.info("Initializing Reddit Processing Service")
    consumer = Consumer()
    logger.info("Starting enrichment pipeline")
    consumer.run()


if __name__ == "__main__":
    main()
