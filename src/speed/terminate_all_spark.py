import os
from pyspark.sql import SparkSession
from .utils.kafka_utils import SPARK_CONNECT_TARGET


def main():
    spark = SparkSession.builder.remote(SPARK_CONNECT_TARGET).getOrCreate()

    print(f"Connected to Spark Server at {SPARK_CONNECT_TARGET}.")
    print("Checking for active streams...")

    # 3. Fetch the list of active streaming queries from the server
    active_streams = spark.streams.active

    # 4. Loop through and stop them
    if not active_streams:
        print("No active streams are currently running.")
    else:
        for active_stream in active_streams:
            # I fixed a small typo from your prompt here (active_clientstream -> active_stream)
            stream_id = active_stream.id
            stream_name = active_stream.name or "Unnamed Stream"

            print(f"Stopping stream... ID: {stream_id} | Name: {stream_name}")

            # Send the termination signal to the server
            active_stream.stop()

        print("All active streams have been successfully terminated.")