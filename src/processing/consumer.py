"""Kafka consumer for processing Reddit events with LLM enrichment"""

import json
import logging
import signal
import time
from typing import List, Tuple, Optional
from pydantic import ValidationError
from kafka import KafkaConsumer, KafkaProducer
from src.shared_utils.config import (
    load_config,
    get_kafka_bootstrap_servers,
    get_kafka_processing_group_id,
    get_kafka_processing_input_topic,
    get_kafka_processing_output_topic,
    get_kafka_dlq_topic,
    get_llm_provider,
    get_llm_api_key,
    get_llm_model,
    get_prompt,
    get_processing_batch_size,
    get_max_retries,
)
from .llm_service import LLMService, get_provider
from src.shared_utils import RedditEvent

logger = logging.getLogger(__name__)


class Consumer:
    """Kafka consumer that enriches Reddit events with LLM"""

    def __init__(self, max_runtime: Optional[float] = None):
        self.config = load_config()
        logger.info("Initializing Consumer")
        self.consumer = KafkaConsumer(
            get_kafka_processing_input_topic(self.config),
            bootstrap_servers=get_kafka_bootstrap_servers(self.config),
            group_id=get_kafka_processing_group_id(self.config),
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            consumer_timeout_ms=-1,
        )
        self.producer = KafkaProducer(
            bootstrap_servers=get_kafka_bootstrap_servers(self.config),
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        self.output_topic = get_kafka_processing_output_topic(self.config)
        self.dlq_topic = get_kafka_dlq_topic(self.config)
        self.batch_size = get_processing_batch_size(self.config)
        self.max_retries = get_max_retries(self.config)
        self.max_runtime = max_runtime

        self.llm_service = LLMService(
            provider=get_provider(
                get_llm_provider(self.config),
                get_llm_api_key(self.config),
                get_llm_model(self.config),
                get_prompt(self.config),
            )
        )
        logger.info(
            f"Consumer ready - batch_size: {self.batch_size}, retries: {self.max_retries}"
        )

        self.batch: List[RedditEvent] = []
        self.running = True

    #  Signal handling (graceful shutdown)
    def _on_sigterm(self, signum, frame):
        logger.info("Received shutdown signal in processing consumer")
        self.running = False

    #  Business logic
    def _process_event_with_retries(self, event: RedditEvent) -> RedditEvent:
        """Process single event with retry logic"""
        for attempt in range(self.max_retries):
            try:
                enriched = self.llm_service.enrich(event)
                if enriched and enriched.enrichment:
                    logger.debug(f"🎉 {event.event_id}: enriched (attempt {attempt + 1})")
                    return enriched
                elif enriched:
                    logger.debug(f"⏭️  {event.event_id}: no enrichment (attempt {attempt + 1})")
                    if attempt < self.max_retries - 1:
                        continue
                    return enriched
                else:
                    logger.debug(f"🔄 {event.event_id}: retry {attempt + 1}/{self.max_retries}")
            except Exception:
                logger.debug(f"⚠️  {event.event_id}: error on attempt {attempt + 1}")
                if attempt == self.max_retries - 1:
                    break

        logger.warning(f"❌ {event.event_id}: enrichment failed (max retries)")
        return event.model_copy(update={"enrichment": None})

    def process_batch(
        self, batch: List[RedditEvent]
    ) -> Tuple[List[RedditEvent], List[RedditEvent]]:
        """Process batch and split into successful and failed"""
        logger.info(f"Processing batch: {len(batch)} events")
        try:
            enriched_batch = self.llm_service.enrich_batch(batch)
        except Exception:
            logger.warning(f"Batch enrichment failed, falling back to individual processing")
            enriched_batch = []
            for event in batch:
                enriched = self._process_event_with_retries(event)
                enriched_batch.append(enriched)

        successful = []
        failed = []
        for enriched in enriched_batch:
            if enriched.enrichment:
                successful.append(enriched)
            else:
                failed.append(enriched)

        return successful, failed

    def shutdown(self, signum=None, frame=None):
        """Handle shutdown signal"""
        if self.running:
            logger.info("Shutdown signal received")
            self.running = False
        if self.batch:
            logger.info(f"Processing {len(self.batch)} remaining events")
            successful, failed = self.process_batch(self.batch)
            for event in successful:
                self.producer.send(self.output_topic, json.loads(event.model_dump_json()))
            logger.info(f"Sent {len(successful)} to output")
            for event in failed:
                self.producer.send(self.dlq_topic, json.loads(event.model_dump_json()))
            logger.info(f"Sent {len(failed)} to DLQ")
            self.producer.flush()
        self.close()

    #  Main loop (Airflow-aware)
    def run(self):
        signal.signal(signal.SIGINT, self._on_sigterm)
        signal.signal(signal.SIGTERM, self._on_sigterm)
        logger.info(f"Listening on: {get_kafka_processing_input_topic(self.config)}")

        start_time = time.time()
        try:
            for message in self.consumer:
                # Stop on signal or max_runtime
                if not self.running:
                    break
                if self.max_runtime and (time.time() - start_time) >= self.max_runtime:
                    logger.info("max_runtime reached, stopping consumer")
                    break
                if not message.value.get("event_id"):
                    continue

                try:
                    event = RedditEvent(**message.value)
                    self.batch.append(event)
                    logger.debug(
                        f"Buffered event {event.event_id} ({len(self.batch)}/{self.batch_size})"
                    )
                except ValidationError as e:
                    logger.error(f"Invalid event format: {e}")
                    continue

                if len(self.batch) >= self.batch_size:
                    successful, failed = self.process_batch(self.batch)
                    for event in successful:
                        self.producer.send(self.output_topic, json.loads(event.model_dump_json()))
                    for event in failed:
                        self.producer.send(self.dlq_topic, json.loads(event.model_dump_json()))

                    self.producer.flush()
                    logger.info(
                        f"Batch processed: {len(successful)} success, {len(failed)} failed"
                    )
                    self.batch = []

            # Process remaining events
            if self.batch and self.running:
                logger.info(f"Processing final batch of {len(self.batch)} events")
                successful, failed = self.process_batch(self.batch)
                for event in successful:
                    self.producer.send(self.output_topic, json.loads(event.model_dump_json()))
                for event in failed:
                    self.producer.send(self.dlq_topic, json.loads(event.model_dump_json()))
                self.producer.flush()
                logger.info(f"Final batch: {len(successful)} success, {len(failed)} failed")
                self.batch = []

        except Exception as e:
            logger.exception(f"Unexpected error in consumer loop: {e}")
            raise

    def close(self):
        """Close consumer and producer"""
        try:
            logger.info("Closing consumer and producer")
            self.consumer.close()
            self.producer.flush()
            self.producer.close()
            logger.info("Consumer shutdown complete")
        except Exception:
            pass
