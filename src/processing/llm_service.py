"""LLM-based text enrichment service"""

import logging
import os
import signal
from typing import Optional, List
from openai import OpenAI
from pydantic import BaseModel
from src.shared_utils import RedditEvent, Enrichment

logger = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)


class TimeoutError(Exception):
    """Custom timeout exception"""
    pass


def timeout_handler(signum, frame):
    """Handle timeout signal"""
    raise TimeoutError("Enrichment operation exceeded timeout")


class BatchEnrichmentResponse(BaseModel):
    """Wrapper for batch LLM response"""

    items: List[Enrichment]


class LLMProvider:
    """Base class for LLM providers"""

    def __init__(self, api_key: str, model: str = "gpt-4o-mini", prompt: str = ""):
        self.api_key = api_key
        self.model = model
        self.prompt = prompt

    def enrich(self, texts: list[str]) -> list[Optional[dict]]:
        raise NotImplementedError


class OpenAIProvider(LLMProvider):
    """OpenAI API provider for enrichment"""

    def __init__(self, api_key: str, model: str = "gpt-4o-mini", prompt: str = ""):
        super().__init__(api_key, model, prompt)
        self.client = OpenAI(api_key=api_key)

    def enrich(self, texts: list[str]) -> list[Optional[dict]]:
        if not texts:
            return []

        combined = "\n\n".join(
            [f"Item {i}:\n{text}" for i, text in enumerate(texts, 1)]
        )
        system_prompt = (
            self.prompt or "Extract sentiment, entities, and topics from each text."
        )
        logger.debug(f"Processing {len(texts)} items with {self.model}")

        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {
                        "role": "user",
                        "content": f"Process these {len(texts)} items:\n\n" + combined,
                    },
                ],
                response_format={
                    "type": "json_schema",
                    "json_schema": {
                        "name": "batch_enrichment_response",
                        "schema": BatchEnrichmentResponse.model_json_schema(),
                    },
                },
                max_tokens=4000,
                timeout=60,
            )
            content = response.choices[0].message.content
            if not content:
                logger.warning("OpenAI returned empty content")
                return [None] * len(texts)
            parsed = BatchEnrichmentResponse.model_validate_json(content)
            logger.debug(f"Processed {len(parsed.items)} items")
            return [item.model_dump() for item in parsed.items]
        except Exception as e:
            logger.warning(f"API error: {type(e).__name__}")
            return [None] * len(texts)


class OpenRouterProvider(LLMProvider):
    """OpenRouter API provider for enrichment"""

    def __init__(
        self,
        api_key: str,
        model: str = "meta-llama/llama-3.3-70b-instruct:free",
        prompt: str = "",
    ):
        super().__init__(api_key, model, prompt)
        self.client = OpenAI(
            api_key=api_key,
            base_url="https://openrouter.ai/api/v1",
            default_headers={
                "HTTP-Referer": "https://reddit-enricher",
                "X-Title": "RedditEnricher",
            },
        )

    def enrich(self, texts: list[str]) -> list[Optional[dict]]:
        if not texts:
            return []

        combined = "\n\n".join(
            [f"Item {i}:\n{text}" for i, text in enumerate(texts, 1)]
        )
        system_prompt = self.prompt

        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {
                        "role": "user",
                        "content": f"Process these {len(texts)} items:\n\n" + combined,
                    },
                ],
                response_format={
                    "type": "json_schema",
                    "json_schema": {
                        "name": "batch_enrichment_response",
                        "schema": BatchEnrichmentResponse.model_json_schema(),
                    },
                },
                max_tokens=4000,
                timeout=60,
            )
            content = response.choices[0].message.content
            if not content:
                logger.warning("OpenRouter returned empty content")
                return [None] * len(texts)

            parsed = BatchEnrichmentResponse.model_validate_json(content)
            logger.debug(
                f"OpenRouter: successfully processed {len(parsed.items)} items"
            )
            return [item.model_dump() for item in parsed.items]
        except Exception as e:
            logger.error(f"OpenRouter error: {type(e).__name__}")
            return [None] * len(texts)
def get_provider(
    provider: str = "openai",
    api_key: str = "",
    model: str = "gpt-4o-mini",
    prompt: str = "",
) -> LLMProvider:
    """Factory function to create LLM provider instances"""
    logger.debug(f"Initializing provider: {provider}")

    if provider == "openai":
        return OpenAIProvider(api_key or os.getenv("OPENAI_API_KEY", ""), model, prompt)
    if provider == "openrouter":
        return OpenRouterProvider(
            api_key or os.getenv("OPENROUTER_API_KEY", ""), model, prompt
        )
    if provider == "dummy":
        from .dummy_provider import DummyProvider
        return DummyProvider(api_key, model, prompt)

    logger.error(f"Unknown provider: {provider}")
    raise ValueError(f"Unknown provider: {provider}")


class LLMService:
    """Service for enriching text using LLM providers"""

    def __init__(self, provider: LLMProvider):
        self.provider = provider

    @staticmethod
    def _extract_text(event: RedditEvent) -> str:
        """Extract text content from Reddit event"""
        return f"{event.title or ''}\n\n{event.content or ''}".strip()

    def enrich_batch(self, events: List[RedditEvent]) -> List[RedditEvent]:
        """Enrich a batch of events with timeout protection"""
        if not events:
            return []
        
        texts = [self._extract_text(event) for event in events]
        
        # Set 90-second timeout for batch enrichment (leaves buffer for polling)
        signal.signal(signal.SIGALRM, timeout_handler)
        signal.alarm(90)
        
        try:
            results = self.provider.enrich(texts)
            signal.alarm(0)  # Cancel alarm
        except TimeoutError:
            logger.warning(f"Batch enrichment timeout after 90s - returning unenriched events")
            signal.alarm(0)  # Cancel alarm
            return events
        except Exception as e:
            signal.alarm(0)  # Cancel alarm
            logger.warning(f"Provider error: {type(e).__name__}")
            return events

        if not results or not isinstance(results, list):
            logger.warning("Invalid provider response")
            return events

        if len(results) != len(events):
            logger.warning(
                f"Result count mismatch: got {len(results)}, expected {len(events)}"
            )

        enriched = []
        for idx, (event, result) in enumerate(zip(events, results)):
            try:
                if result and isinstance(result, dict):
                    enrichment = Enrichment(**result)
                    enriched.append(event.model_copy(update={"enrichment": enrichment}))
                else:
                    logger.debug(f"Event {event.event_id}: empty or null result")
                    enriched.append(event.model_copy(update={"enrichment": None}))
            except Exception as e:
                logger.warning(f"Event {event.event_id}: {type(e).__name__}")
                enriched.append(event.model_copy(update={"enrichment": None}))

        return enriched

    def enrich(self, event: RedditEvent) -> Optional[RedditEvent]:
        """Enrich a single event with timeout protection"""
        text = self._extract_text(event)
        if not text:
            return event.model_copy(update={"enrichment": None})

        # Set 30-second timeout for single event enrichment
        signal.signal(signal.SIGALRM, timeout_handler)
        signal.alarm(30)
        
        try:
            result = self.provider.enrich([text])
            signal.alarm(0)  # Cancel alarm
        except TimeoutError:
            logger.warning(f"Single event enrichment timeout after 30s for {event.event_id}")
            signal.alarm(0)  # Cancel alarm
            return event.model_copy(update={"enrichment": None})
        except Exception as e:
            signal.alarm(0)  # Cancel alarm
            logger.debug(f"Provider error: {type(e).__name__}")
            return None

        if result and isinstance(result, list) and len(result) > 0:
            item = result[0]
            if isinstance(item, dict):
                try:
                    enrichment = Enrichment(**item)
                    return event.model_copy(update={"enrichment": enrichment})
                except Exception as e:
                    logger.debug(f"Event {event.event_id}: validation error")
                    return event.model_copy(update={"enrichment": None})
            else:
                logger.debug(f"Event {event.event_id}: invalid result format")
                return event.model_copy(update={"enrichment": None})

        return event.model_copy(update={"enrichment": None})
