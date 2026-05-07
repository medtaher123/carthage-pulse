"""Shared configuration - common config loaders and getters"""

import os
from pathlib import Path
from typing import Optional

import yaml

from src.shared_utils.config_utils import expand_env_vars


# Calculate absolute path to project root
ROOT_DIR = Path(__file__).parent.parent.parent
DEFAULT_CONFIG_PATH = str(ROOT_DIR / "config" / "dev.yaml")
_cached_config: Optional[dict] = None


def load_config(config_path: str = DEFAULT_CONFIG_PATH, use_cache: bool = True) -> dict:
    """Load YAML configuration file with env var expansion."""
    global _cached_config

    if use_cache and _cached_config is not None:
        return _cached_config

    path = Path(config_path)
    if path.exists():
        with open(path) as f:
            config = yaml.safe_load(f) or {}
            print(f"DEBUG: Loaded config from {config_path}: {config}")
            config = expand_env_vars(config)
            print(f"DEBUG: Expanded config: {config}")
            if use_cache:
                _cached_config = config
            return config
    print(f"DEBUG: Config file not found at {config_path}")
    return {}


def clear_config_cache():
    """Clear cached config (useful for testing)."""
    global _cached_config
    _cached_config = None


# === Kafka ===

def get_kafka_bootstrap_servers(config: Optional[dict] = None) -> list:
    if config is None:
        config = load_config()
    return config.get("kafka", {}).get("bootstrap_servers", ["localhost:9092"])


def get_kafka_group_id(config: Optional[dict] = None) -> str:
    if config is None:
        config = load_config()
    return config.get("kafka", {}).get("group_id", "reddit-group")


def get_kafka_topic(config: Optional[dict] = None) -> str:
    if config is None:
        config = load_config()
    return config.get("kafka", {}).get("ingestion_topic", "reddit-events")


# === PostgreSQL ===

def get_postgres_host(config: Optional[dict] = None) -> str:
    if config is None:
        config = load_config()
    return config.get("postgres", {}).get("host", "localhost")


def get_postgres_port(config: Optional[dict] = None) -> int:
    if config is None:
        config = load_config()
    return config.get("postgres", {}).get("port", 5432)


def get_postgres_user(config: Optional[dict] = None) -> str:
    if config is None:
        config = load_config()
    return config.get("postgres", {}).get("user", "reddit")


def get_postgres_password(config: Optional[dict] = None) -> str:
    if config is None:
        config = load_config()
    return config.get("postgres", {}).get("password", "reddit")


def get_postgres_db(config: Optional[dict] = None) -> str:
    if config is None:
        config = load_config()
    return config.get("postgres", {}).get("database", "reddit")


# === MinIO ===

def get_minio_endpoint(config: Optional[dict] = None) -> str:
    if config is None:
        config = load_config()
    return config.get("minio", {}).get("endpoint", "localhost:9000")


def get_minio_access_key(config: Optional[dict] = None) -> str:
    if config is None:
        config = load_config()
    return config.get("minio", {}).get("access_key", "minioadmin")


def get_minio_secret_key(config: Optional[dict] = None) -> str:
    if config is None:
        config = load_config()
    return config.get("minio", {}).get("secret_key", "minioadmin")


def get_minio_bucket(config: Optional[dict] = None) -> str:
    if config is None:
        config = load_config()
    return config.get("minio", {}).get("bucket", "reddit-events")


def get_minio_secure(config: Optional[dict] = None) -> bool:
    if config is None:
        config = load_config()
    return config.get("minio", {}).get("secure", False)


# === Ingestion ===

def get_subreddits(config: Optional[dict] = None) -> list:
    if config is None:
        config = load_config()
    return config.get("ingestion", {}).get("subreddits", ["Tunisia"])


def get_poll_interval(config: Optional[dict] = None) -> int:
    if config is None:
        config = load_config()
    return config.get("ingestion", {}).get("poll_interval", 30)


def get_fetch_limit(config: Optional[dict] = None) -> int:
    if config is None:
        config = load_config()
    return config.get("ingestion", {}).get("fetch_limit", 100)


def get_user_agent(config: Optional[dict] = None) -> str:
    if config is None:
        config = load_config()
    return config.get("ingestion", {}).get("user_agent", "reddit-rss/1.0")


def get_endpoints(config: Optional[dict] = None) -> list:
    if config is None:
        config = load_config()
    return config.get("ingestion", {}).get("endpoints", ["new", "comments"])


def get_initial_fetch(config: Optional[dict] = None) -> bool:
    if config is None:
        config = load_config()
    return config.get("ingestion", {}).get("initial_fetch", True)


def build_subreddit_url(subreddits: list) -> str:
    return "+".join(subreddits)


# === Speed Layer ===

def get_window_seconds(config: Optional[dict] = None) -> int:
    if config is None:
        config = load_config()
    return config.get("speed", {}).get("window_seconds", 60)


def get_late_event_grace_seconds(config: Optional[dict] = None) -> int:
    if config is None:
        config = load_config()
    return config.get("speed", {}).get("late_event_grace_seconds", 30)


def get_max_events_per_window(config: Optional[dict] = None) -> int:
    if config is None:
        config = load_config()
    return config.get("speed", {}).get("max_events_per_window", 10000)


# === Processing ===

def get_llm_provider(config: Optional[dict] = None) -> str:
    if config is None:
        config = load_config()
    return config.get("llm", {}).get("provider", "openrouter")


def get_llm_api_key(config: Optional[dict] = None) -> str:
    if config is None:
        config = load_config()
    return config.get("llm", {}).get("api_key", "")


def get_llm_model(config: Optional[dict] = None) -> str:
    if config is None:
        config = load_config()
    return config.get("llm", {}).get("model", "gpt-4o-mini")


def get_kafka_processing_group_id(config: Optional[dict] = None) -> str:
    if config is None:
        config = load_config()
    return config.get("kafka", {}).get("processing_group_id", "reddit-enricher-group")


def get_kafka_processing_input_topic(config: Optional[dict] = None) -> str:
    if config is None:
        config = load_config()
    return config.get("kafka", {}).get("processing_input_topic", "reddit-events")


def get_kafka_processing_output_topic(config: Optional[dict] = None) -> str:
    if config is None:
        config = load_config()
    return config.get("kafka", {}).get(
        "processing_output_topic", "reddit-events-enriched"
    )


def get_kafka_dlq_topic(config: Optional[dict] = None) -> str:
    if config is None:
        config = load_config()
    return config.get("kafka", {}).get("processing_dlq_topic", "reddit-events-dlq")


def get_processing_batch_size(config: Optional[dict] = None) -> int:
    if config is None:
        config = load_config()
    return config.get("processing", {}).get("batch_size", 5)


def get_max_retries(config: Optional[dict] = None) -> int:
    if config is None:
        config = load_config()
    return config.get("processing", {}).get("max_retries", 3)


def get_prompt(config: Optional[dict] = None) -> str:
    if config is None:
        config = load_config()
    return config.get("prompt", "")


# === Storage ===

def get_kafka_storage_group_id(config: Optional[dict] = None) -> str:
    if config is None:
        config = load_config()
    return config.get("kafka", {}).get("storage_group_id", "reddit-storage-group")


def get_kafka_storage_input_topic(config: Optional[dict] = None) -> str:
    if config is None:
        config = load_config()
    return config.get("kafka", {}).get("storage_input_topic", "reddit-events-enriched")


def get_storage_batch_size(config: Optional[dict] = None) -> int:
    if config is None:
        config = load_config()
    return config.get("storage", {}).get("batch_size", 10)


# === Speed Layer Kafka Topics ===

def get_kafka_speed_input_topic(config: Optional[dict] = None) -> str:
    if config is None:
        config = load_config()
    return config.get("kafka", {}).get("speed_input_topic", "reddit-events-enriched")