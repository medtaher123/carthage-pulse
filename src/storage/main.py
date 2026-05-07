"""Main entry point for Reddit storage service"""

import logging
import sys
import warnings
from typing import Optional
from src.storage.consumer import StorageConsumer
from src.shared_utils import setup_logging

warnings.filterwarnings("ignore", category=DeprecationWarning)

logger = setup_logging(logging.INFO)


def main(max_runtime: Optional[float] = None):
    """
    Start the storage service.

    Args:
        max_runtime: If given, the service will gracefully exit after
                     this many seconds.
    """
    logger.info("Initializing Reddit Storage Service")
    consumer = StorageConsumer(max_runtime=max_runtime)
    try:
        logger.info("Starting storage pipeline")
        consumer.run()
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
    sys.exit(0)
