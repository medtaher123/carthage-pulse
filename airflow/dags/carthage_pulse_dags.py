"""Airflow DAGs for Carthage Pulse - Reddit Analytics Pipeline

This module defines three main DAGs:
1. reddit_streaming_pipeline: Orchestrates long-running services (ingestion, processing, storage, speed)
2. reddit_batch_pipeline: Schedules batch jobs (daily, hourly, weekly)
3. reddit_monitoring_dag: Health checks for infrastructure services
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator
from airflow.utils.task_group import TaskGroup

# Default arguments for all DAGs
default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
    'email': [],
    'email_on_failure': False,
    'email_on_retry': False,
}

# Constants for task execution
PYTHON_CMD = '/opt/airflow/.venv/bin/python'
PROJECT_PATH = '/opt/airflow/project'
RUN_TASK_SCRIPT = f'{PROJECT_PATH}/airflow/run_task.py'

# DAG 1: Streaming Pipeline (runs every hour, services restart hourly)
with DAG(
    'reddit_streaming_pipeline',
    default_args=default_args,
    description='Orchestrate all long-running services (ingestion, processing, storage, speed)',
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['reddit', 'streaming', 'carthage-pulse'],
    max_active_tasks=32,
    max_active_runs=1,
) as dag_stream:

    # TaskGroup for core services (ingestion, processing, storage)
    with TaskGroup("services", tooltip="Core long-running services") as services_group:
        run_ingestion = BashOperator(
            task_id='run_ingestion',
            bash_command=f'{PYTHON_CMD} {RUN_TASK_SCRIPT} ingestion --max-runtime 3300',
        )

        run_processing = BashOperator(
            task_id='run_processing',
            bash_command=f'{PYTHON_CMD} {RUN_TASK_SCRIPT} processing --max-runtime 3300',
        )

        run_storage = BashOperator(
            task_id='run_storage',
            bash_command=f'{PYTHON_CMD} {RUN_TASK_SCRIPT} storage --max-runtime 3300',
        )

    # TaskGroup for speed layer (Spark streaming jobs)
    with TaskGroup("speed", tooltip="Spark Streaming jobs") as speed_group:
        run_speed_raw = BashOperator(
            task_id='run_speed_save_raw',
            bash_command=f'{PYTHON_CMD} {RUN_TASK_SCRIPT} speed_save_raw --max-runtime 3300',
        )

        run_speed_enriched = BashOperator(
            task_id='run_speed_save_enriched',
            bash_command=f'{PYTHON_CMD} {RUN_TASK_SCRIPT} speed_save_enriched --max-runtime 3300',
        )

        run_speed_trending_topics = BashOperator(
            task_id='run_speed_trending_topics',
            bash_command=f'{PYTHON_CMD} {RUN_TASK_SCRIPT} speed_trending_topics --max-runtime 3300',
        )

        run_speed_trending_words = BashOperator(
            task_id='run_speed_trending_words',
            bash_command=f'{PYTHON_CMD} {RUN_TASK_SCRIPT} speed_trending_words --max-runtime 3300',
        )

    # Dependencies: services and speed run in parallel (no strict dependency for resilience)
    # In production, you might want speed to depend on services being healthy

# DAG 2: Batch Pipeline (runs daily at midnight)
with DAG(
    'reddit_batch_pipeline',
    default_args=default_args,
    description='Run daily/hourly batch jobs on enriched data',
    schedule_interval='0 0 * * *',  # Daily at midnight
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['reddit', 'batch', 'carthage-pulse'],
) as dag_batch:

    run_daily_sentiment = BashOperator(
        task_id='run_daily_sentiment',
        bash_command=f'{PYTHON_CMD} {RUN_TASK_SCRIPT} batch_daily_sentiment',
    )

    run_hourly_sentiment = BashOperator(
        task_id='run_hourly_sentiment',
        bash_command=f'{PYTHON_CMD} {RUN_TASK_SCRIPT} batch_hourly_sentiment',
    )

    run_topic_sentiment = BashOperator(
        task_id='run_topic_sentiment',
        bash_command=f'{PYTHON_CMD} {RUN_TASK_SCRIPT} batch_topic_sentiment',
    )

    run_top_posts = BashOperator(
        task_id='run_top_posts',
        bash_command=f'{PYTHON_CMD} {RUN_TASK_SCRIPT} batch_top_posts',
    )

    # Branch for weekly tasks (only run on Sundays)
    def check_weekday(**kwargs):
        execution_date = kwargs['execution_date']
        if execution_date.weekday() == 6:  # Sunday
            return 'run_weekly_topics'
        return 'skip_weekly_topics'

    branch_weekly = BranchPythonOperator(
        task_id='branch_weekly',
        python_callable=check_weekday,
    )

    run_weekly_topics = BashOperator(
        task_id='run_weekly_topics',
        bash_command=f'{PYTHON_CMD} {RUN_TASK_SCRIPT} batch_weekly_topics',
    )

    skip_weekly = BashOperator(
        task_id='skip_weekly_topics',
        bash_command='echo "Not Sunday, skipping weekly_topics"',
    )

    # Dependencies: sequential execution
    run_daily_sentiment >> run_hourly_sentiment >> run_topic_sentiment >> run_top_posts >> branch_weekly
    branch_weekly >> run_weekly_topics
    branch_weekly >> skip_weekly

# DAG 3: Monitoring DAG (runs every 5 minutes)
with DAG(
    'reddit_monitoring_dag',
    default_args=default_args,
    description='Health checks for infrastructure services',
    schedule_interval=timedelta(minutes=5),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['reddit', 'monitoring', 'carthage-pulse'],
) as dag_monitor:

    check_kafka = BashOperator(
        task_id='check_kafka',
        bash_command='nc -z kafka 9093 || exit 1',
    )

    check_postgres = BashOperator(
        task_id='check_postgres',
        bash_command='nc -z postgres 5432 || exit 1',
    )

    check_minio = BashOperator(
        task_id='check_minio',
        bash_command='nc -z minio 9000 || exit 1',
    )

    check_spark_master = BashOperator(
        task_id='check_spark_master',
        bash_command='nc -z spark-master 7077 || exit 1',
    )

    check_spark_connect = BashOperator(
        task_id='check_spark_connect',
        bash_command='nc -z spark-connect 15002 || exit 1',
    )
