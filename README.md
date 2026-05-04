# Carthage Pulse

Real-time Reddit analytics pipeline with LLM-powered sentiment analysis, topic extraction, and entity recognition — focused on Tunisian social media content.

## Architecture

Carthage Pulse follows a Lambda Architecture with batch and speed layers for real-time and historical analytics. Reddit posts and comments are ingested from the API and published to Kafka. A processing service consumes these events, enriches them with LLM analysis (sentiment, topics, entities, translation), and publishes enriched events back to Kafka. A storage service persists both raw and enriched data to MinIO and PostgreSQL. Spark Streaming jobs consume from Kafka to compute real-time trending topics and words, while Spark Batch jobs handle historical analysis. Results are stored in PostgreSQL and visualized through Grafana dashboards.

## Services

| Service | Description |
|---------|-------------|
| `ingestion` | Fetches posts/comments from Reddit API and publishes to Kafka |
| `processing` | Consumes Kafka events, enriches with LLM analysis (sentiment, topics, entities, translation) |
| `storage` | Persists enriched events to MinIO (object storage) |
| `speed` | Spark Streaming jobs for real-time analytics (trending topics, trending words) |
| `batch` | Spark Batch jobs for historical analysis (coming soon) |
| `presentation` | Grafana dashboards for real-time metrics and trends |

## Presentation

Grafana dashboards (available at http://localhost:3000, login: admin/admin) display real-time metrics:

- **Raw events** — time series chart and gauge showing ingestion rate over time
- **Enriched events** — time series chart and gauge showing processing throughput
- **Rolling averages** — 5-minute moving average of event rates

## Quick Start

### Prerequisites

- Docker & Docker Compose
- Python 3.10+
- [uv](https://github.com/astral-sh/uv) package manager

### 1. Start Infrastructure

```bash
docker compose up -d
```

### 2. Configure

```bash
cp .env.example .env
# Edit .env with your API keys
```

Review `config/dev.yaml` for pipeline settings (subreddits, LLM provider, batch sizes, etc.).

### 3. Run the Pipeline

```bash
uv sync
python main.py
```

A TUI menu will appear with options to run each service.
