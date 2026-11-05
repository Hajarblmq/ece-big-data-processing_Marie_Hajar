"""Shared SparkSession factory."""
from pyspark.sql import SparkSession


def get_spark(app_name: str = "ECE-BigData") -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )
