import os
import psycopg2
from pyspark.sql import SparkSession
from src.shared_utils.config import (
    load_config,
    get_postgres_host,
    get_postgres_port,
    get_postgres_user,
    get_postgres_password,
    get_postgres_db,
    get_minio_access_key,
    get_minio_secret_key,
    get_minio_bucket
)
from src.speed.utils.kafka_utils import SPARK_CONNECT_TARGET

def get_spark_batch_session(app_name: str) -> SparkSession:
    config = load_config()
    
    # We connect natively to spark connect
    # S3 configurations for MinIO need to be passed to the session
    # We assume 'minio' resolves to the minio container correctly for the spark-master, 
    # but could be localhost if run locally. The actual reads happen on the server/executors,
    # so 'minio:9000' is the right host for the docker network.
    minio_endpoint = os.getenv("MINIO_INTERNAL_ENDPOINT", "http://minio:9000")
    minio_access = get_minio_access_key(config)
    minio_secret = get_minio_secret_key(config)
    
    spark = (
        SparkSession.builder
        .appName(app_name)
        .remote(SPARK_CONNECT_TARGET)
        .getOrCreate()
    )
    return spark

def setup_timescale_hypertable(table_name: str, create_sql: str, time_column: str):
    """Ensure the table exists and is converted to a hypertable."""
    config = load_config()
    db_name = get_postgres_db(config)
    db_user = get_postgres_user(config)
    db_pass = get_postgres_password(config)
    db_host = get_postgres_host(config)
    db_port = str(get_postgres_port(config))

    print(f"Connecting to Postgres at {db_host}:{db_port}...")
    try:
        conn = psycopg2.connect(
            dbname=db_name,
            user=db_user,
            password=db_pass,
            host=db_host,
            port=db_port,
            connect_timeout=5
        )
    except Exception as e:
        print(f"Could not connect to Postgres to setup hypertable {table_name}: {e}")
        return
            
    conn.autocommit = True
    cur = conn.cursor()
    try:
        # Check if table already exists
        cur.execute(f"SELECT to_regclass('{table_name}');")
        exists = cur.fetchone()[0]
        
        if not exists:
            cur.execute(create_sql)
            print(f"Table {table_name} created.")
        else:
            print(f"Table {table_name} already exists, skipping creation.")

        # Check if it's already a hypertable
        cur.execute(f"SELECT * FROM timescaledb_information.hypertables WHERE hypertable_name = '{table_name}';")
        is_hyper = cur.fetchone()
        
        if not is_hyper:
            cur.execute(f"SELECT create_hypertable('{table_name}', '{time_column}', if_not_exists => TRUE, migrate_data => TRUE);")
            print(f"Hypertable {table_name} setup complete.")
    except Exception as e:
        print(f"Error during hypertable setup for {table_name}: {e}")
    finally:
        cur.close()
        conn.close()

def write_batch_to_postgres(df, db_table: str, mode: str = "append"):
    db_url = os.getenv("POSTGRES_INTERNAL_URL", "jdbc:postgresql://postgres:5432/reddit")
    db_user = os.getenv("POSTGRES_USER", "reddit")
    db_pass = os.getenv("POSTGRES_PASSWORD", "reddit")

    (
        df.write
        .format("jdbc")
        .option("url", db_url)
        .option("dbtable", db_table)
        .option("user", db_user)
        .option("password", db_pass)
        .option("driver", "org.postgresql.Driver")
        .mode(mode)
        .save()
    )
