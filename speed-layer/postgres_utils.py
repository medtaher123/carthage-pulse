from pyspark.sql import DataFrame
import os


def get_postgres_writer(db_url: str, db_name: str, db_table: str, db_user: str, db_pass: str, conflict_columns: list) -> DataFrame:
    """Returns a closure to be used in foreachBatch."""

    def write_to_postgres(batch_df: DataFrame, batch_id: int):
        import psycopg2

        print("-------------------------------------------")
        print(f"BATCH ID: {batch_id} | TABLE: {db_table}")
        count = batch_df.count()
        print(f"RECORDS RECEIVED: {count}")

        if count == 0:
            return

        staging_table = f"staging_events_{db_table}_{batch_id}"
        (
            batch_df.write
            .format("jdbc")
            .option("url", db_url)
            .option("dbtable", staging_table)
            .option("user", db_user)
            .option("password", db_pass)
            .option("driver", "org.postgresql.Driver")
            #.option("createTableColumnTypes", "entities JSONB")
            #.option("stringtype", "unspecified")
            .mode("overwrite")  # Overwrite the temp table each time
            .save()
        )
        # 2. Execute raw SQL to merge staging table into the target table, ignoring duplicates
        conflict_columns_str = ", ".join(conflict_columns)
        merge_query = f"""
                INSERT INTO {db_table}
                SELECT * FROM {staging_table}
                ON CONFLICT ({conflict_columns_str}) DO NOTHING;

                DROP TABLE {staging_table};
            """

        # Execute the merge query using psycopg2
        try:
            conn = psycopg2.connect(
                dbname=db_name,
                user=db_user,
                password=db_pass,
                host="postgres",
                port="5432"
            )
            cur = conn.cursor()
            cur.execute(merge_query)
            conn.commit()
            cur.close()
            conn.close()
            print(f"Successfully merged {count} from {staging_table} to {db_table} records and dropped staging table.")
        except Exception as e:
            print(f"Failed to merge batch {batch_id} from {staging_table} to {db_table}: {e}")

    return write_to_postgres


def write_to_postgres(df, checkpoint_id, db_table, conflict_columns=("event_id", "timestamp"), checkpoint_base_dir="/data/checkpoints/spark_checkpoints"):
    """Writes a streaming DataFrame to Postgres. Completely independent of the source."""

    # Database credentials
    db_url = os.getenv("POSTGRES_URL", "jdbc:postgresql://postgres:5432/reddit")
    db_name = os.getenv("POSTGRES_DATABASE", "reddit")
    db_user = os.getenv("POSTGRES_USER", "reddit")
    db_pass = os.getenv("POSTGRES_PASSWORD", "reddit")

    # The checkpoint is now based on a unique ID you provide, NOT the Kafka topic
    checkpoint_location = f"{checkpoint_base_dir}__{db_table}__{checkpoint_id}"

    writer_func = get_postgres_writer(db_url, db_name, db_table, db_user, db_pass, conflict_columns)

    query = (
        df.writeStream
        .outputMode("append")
        .foreachBatch(writer_func)
        .option("checkpointLocation", checkpoint_location)
        .start()
    )

    return query

