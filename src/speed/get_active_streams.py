import os
from pyspark.sql import SparkSession
from .utils.kafka_utils import SPARK_CONNECT_TARGET


def main():
    spark = SparkSession.builder.remote(SPARK_CONNECT_TARGET).getOrCreate()

    print(f"Connected to Spark Server at {SPARK_CONNECT_TARGET}.")
    print("Checking for active streams...")

    active_streams = spark.streams.active

    if not active_streams:
        print("No active streams are currently running.")
    else:
        for active_stream in active_streams:
            stream_id = active_stream.id
            stream_name = active_stream.name or "Unnamed Stream"

            print(f"Stream {stream_id}: {stream_name}")


        print("All active streams have been successfully listed.")