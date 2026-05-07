# Carthage Pulse

Real-time Reddit analytics pipeline with LLM-powered sentiment analysis, topic extraction, and entity recognition — focused on Tunisian social media content.

## Architecture

Carthage Pulse follows a Lambda Architecture with batch and speed layers for real-time and historical analytics. Reddit posts and comments are ingested from the API and published to Kafka. A processing service consumes these events, enriches them with LLM analysis (sentiment, topics, entities, translation), and publishes enriched events back to Kafka. A storage service persists both raw and enriched data to MinIO and PostgreSQL. Spark Streaming jobs consume from Kafka to compute real-time trending topics and words, while Spark Batch jobs handle historical analysis. Results are stored in PostgreSQL and visualized through Grafana dashboards.

**Airflow Integration**: The entire pipeline is now orchestrated by Apache Airflow, providing automated scheduling, monitoring, and retry logic for all services.

## Services

| Service | Description |
|---------|-------------|
| `ingestion` | Fetches posts/comments from Reddit API and publishes to Kafka |
| `processing` | Consumes Kafka events, enriches with LLM analysis (sentiment, topics, entities, translation) |
| `storage` | Persists enriched events to MinIO (object storage) |
| `speed` | Spark Streaming jobs for real-time analytics (trending topics, trending words) |
| `batch` | Spark Batch jobs for historical analysis (daily, hourly, weekly) |
| `airflow` | Apache Airflow orchestrator for automated pipeline management |
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

This starts all services including:
- Kafka, MinIO, PostgreSQL, Spark
- Airflow webserver and scheduler
- Grafana dashboards

### 2. Configure

```bash
cp .env.example .env
# Edit .env with your API keys
```

Review `config/dev.yaml` for pipeline settings (subreddits, LLM provider, batch sizes, etc.).

### 3. Run the Pipeline with Airflow

**Access Airflow UI:**
```
http://localhost:8088
```
Login: `admin` / `admin`

**Available DAGs:**

| DAG | Schedule | Purpose |
|-----|----------|---------|
| `reddit_streaming_pipeline` | Every hour | Orchestrates long-running services (ingestion, processing, storage, speed) |
| `reddit_batch_pipeline` | Daily at midnight | Runs batch jobs (daily/hourly sentiment, topics, top posts, weekly) |
| `reddit_monitoring_dag` | Every 5 minutes | Health checks for Kafka, Postgres, MinIO, Spark |

**To start the pipeline:**
1. Open Airflow UI at http://localhost:8088
2. Credentials: admin/admin
3. Navigate to DAGs → `reddit_streaming_pipeline`
4. Click "Trigger DAG Run"
5. All services will start automatically and restart every hour

**Features:**
- ✅ **Auto-restart**: Services restart automatically on failure (3 retries, 5min delay)
- ✅ **Centralized monitoring**: Single dashboard for all pipeline status
- ✅ **Historical logs**: Every execution is logged and searchable
- ✅ **Graceful shutdown**: Services handle SIGTERM and exit cleanly

### 4. Manual Mode (Development/Testing)

For manual testing or development, you can still run services individually:

```bash
uv sync
python main.py
```

A TUI menu will appear with options to run each service manually.

## DAG Details

### Streaming Pipeline (`reddit_streaming_pipeline`)

Runs every hour and orchestrates:
- **Ingestion**: Streams Reddit posts to Kafka (max 55 min runtime)
- **Processing**: Enriches events with LLM analysis (max 55 min runtime)
- **Storage**: Persists enriched events to MinIO (max 55 min runtime)
- **Speed Layer**: Spark streaming jobs for real-time analytics (max 55 min runtime)

### Batch Pipeline (`reddit_batch_pipeline`)

Runs daily at midnight and executes:
- Daily sentiment analysis
- Hourly sentiment analysis
- Topic sentiment analysis
- Top posts leaderboard
- Weekly topics extraction (Sundays only)

### Monitoring DAG (`reddit_monitoring_dag`)

Runs every 5 minutes and checks:
- Kafka connectivity
- PostgreSQL connectivity
- MinIO connectivity
- Spark master and worker status

## Troubleshooting

### Airflow UI not accessible
```bash
docker compose logs airflow_webserver
```

### Services not starting
```bash
docker compose ps
docker compose logs <service_name>
```

### DAGs not appearing
```bash
docker exec airflow_webserver airflow dags list
```

### Manual service testing
```bash
python main.py
# Select the service you want to test
```

## Development

### Adding new services

1. Create the service in `src/<service>/main.py`
2. Add `max_runtime` parameter support using `ServiceRunner`
3. Update `airflow/run_task.py` to include the new service
4. Add the service to the appropriate DAG in `airflow/dags/carthage_pulse_dags.py`

### Testing DAGs locally

```bash
docker exec airflow_webserver airflow dags test <dag_id> <execution_date>
```

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                        Airflow Orchestrator                      │
│  ┌──────────────────────┐  ┌──────────────────────┐              │
│  │ Streaming Pipeline   │  │   Batch Pipeline     │              │
│  │  (hourly)            │  │   (daily)            │              │
│  └──────────────────────┘  └──────────────────────┘              │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                          Kafka Topics                            │
│  ┌──────────────┐  ┌──────────────────────┐  ┌──────────────┐  │
│  │ reddit-events│  │reddit-events-enriched│  │reddit-events-│  │
│  │              │  │                      │  │    dlq       │  │
│  └──────────────┘  └──────────────────────┘  └──────────────┘  │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                      Processing Services                          │
│  ┌──────────────┐  ┌──────────────────────┐  ┌──────────────┐  │
│  │  Ingestion   │  │     Processing        │  │    Storage    │  │
│  │              │  │   (LLM Enrichment)    │  │   (MinIO)     │  │
│  └──────────────┘  └──────────────────────┘  └──────────────┘  │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                      Analytics Layer                             │
│  ┌──────────────────────┐  ┌──────────────────────┐              │
│  │   Speed Layer        │  │    Batch Layer       │              │
│  │ (Spark Streaming)    │  │  (Spark Batch)        │              │
│  └──────────────────────┘  └──────────────────────┘              │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                      Storage & Presentation                      │
│  ┌──────────────┐  ┌──────────────────────┐  ┌──────────────┐  │
│  │  PostgreSQL  │  │       MinIO          │  │    Grafana   │  │
│  │ (TimescaleDB)│  │   (Object Storage)   │  │  (Dashboards) │  │
│  └──────────────┘  └──────────────────────┘  └──────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```
