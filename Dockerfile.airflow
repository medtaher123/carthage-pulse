FROM python:3.10-slim

WORKDIR /opt/airflow

# --------------------------------------------------------------------------- #
# System deps for building Python packages (psycopg2, PyArrow, etc.)
# --------------------------------------------------------------------------- #
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    libpq-dev \
    wget \
    curl \
    git \
    default-jre \
    && rm -rf /var/lib/apt/lists/*

# Install uv (fast Python package manager)
RUN pip install --no-cache-dir uv

# --------------------------------------------------------------------------- #
# Install Python deps in a virtual environment
# --------------------------------------------------------------------------- #
COPY pyproject.toml .
RUN uv venv .venv
RUN . .venv/bin/activate && uv pip install .

# Ensure the venv is on PATH so `python` / `airflow` work everywhere
ENV PATH="/opt/airflow/.venv/bin:$PATH"
ENV AIRFLOW_HOME=/opt/airflow
ENV PYTHONPATH=/opt/airflow/project

# Expose Airflow webserver port
EXPOSE 8080

