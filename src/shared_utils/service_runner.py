"""ServiceRunner: A reusable, signal-aware runner for orchestrating long-running services with Airflow

Usage:
    runner = ServiceRunner(poll_interval=30, max_runtime=3600)
    runner.run(my_callback)

Features:
- Handles SIGINT/SIGTERM gracefully
- Supports max_runtime to let Airflow restart processes periodically
- Responsive sleep that breaks early on shutdown signals
"""

import signal
import time
import logging
from typing import Callable, Optional

logger = logging.getLogger(__name__)


class ServiceRunner:
    """Runs a callback loop with graceful shutdown and optional max runtime."""

    def __init__(
        self,
        poll_interval: float = 1,
        max_runtime: Optional[float] = None,
    ):
        self.poll_interval = poll_interval
        self.max_runtime = max_runtime
        self.start_time = time.time()
        self.running = True
        self.shutdown_reason = ""

        try:
            signal.signal(signal.SIGTERM, self._on_shutdown_signal)
            signal.signal(signal.SIGINT, self._on_shutdown_signal)
        except ValueError:
            # Signals unavailable (threads, Windows sometimes)
            pass

    #  Signal handling
    def _on_shutdown_signal(self, signum, frame):
        logger.info(f"Received signal {signum}, initiating graceful shutdown...")
        self.running = False
        self.shutdown_reason = f"signal:{signum}"

    #  Lifecycle helpers
    def should_continue(self) -> bool:
        """Return True if the service should keep running."""
        if not self.running:
            return False
        if self.max_runtime and (time.time() - self.start_time) >= self.max_runtime:
            self.shutdown_reason = "max_runtime"
            return False
        return True

    def sleep_step(self, duration: float = 1.0) -> None:
        """Sleep for *duration* seconds, but wake up early if runner.stop() was called."""
        if not self.running:
            return
        deadline = time.time() + duration
        while self.running and time.time() < deadline:
            time.sleep(min(0.5, deadline - time.time()))

    def run(self, callback: Callable) -> None:
        """Run *callback* repeatedly until stopped or max_runtime reached."""
        iteration = 0
        while self.should_continue():
            try:
                callback()
            except Exception:
                logger.exception(f"Error during service callback (iteration {iteration})")

            # Always sleep between iterations to respect the poll_interval
            self.sleep_step(self.poll_interval)
            iteration += 1
        logger.info(
            f"Service exiting. {iteration} iterations, "
            f"runtime={time.time() - self.start_time:.1f}s "
            f"(reason={self.shutdown_reason})"
        )
