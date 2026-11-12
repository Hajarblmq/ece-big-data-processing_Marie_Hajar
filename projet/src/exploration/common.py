"""Shared helpers for the exploration queries."""
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

PROJECT_DIR = Path(__file__).resolve().parents[2]
PROCESSED_DIR = PROJECT_DIR / "data" / "processed"
OUTPUT_DIR = PROJECT_DIR / "output" / "exploration"


def load_clean(spark: SparkSession, table: str,
               drop_flagged: bool = True) -> DataFrame:
    """Read a cleaned Parquet table, optionally dropping flagged rows."""
    df = spark.read.parquet(str(PROCESSED_DIR / table))
    if drop_flagged:
        df = df.filter(F.size("_quality_flags") == 0)
    return df


def save_csv(df: DataFrame, name: str) -> None:
    """Write a small result as a single CSV file under output/exploration."""
    (df.coalesce(1).write.mode("overwrite").option("header", "true")
       .csv(str(OUTPUT_DIR / name)))
