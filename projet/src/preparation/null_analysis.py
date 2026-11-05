"""Compute null / missing / empty ratio per column across all IMDb tables.

Answers Q1.1 (null ratio per column per table) and Q1.2 (top 5 columns
across the whole dataset with the highest null ratio).
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, count, trim, when

from ..config import IMDB_SCHEMAS
from ..io_utils import read_imdb


def per_column_null_ratio(df: DataFrame, table: str) -> DataFrame:
    """Return (table, column, null_pct) for every column of df."""
    n_total = df.count()
    aggs = [
        count(
            when(col(c).isNull() | (trim(col(c).cast("string")) == ""), c)
        ).alias(c)
        for c in df.columns
    ]
    row = df.agg(*aggs).collect()[0]
    data = [(table, c, row[c] / n_total * 100.0) for c in df.columns]
    return df.sparkSession.createDataFrame(
        data, ["table", "column", "null_pct"]
    )


def run(spark: SparkSession) -> DataFrame:
    """Iterate over all IMDb tables and return a consolidated null-ratio DataFrame."""
    results = [
        per_column_null_ratio(read_imdb(spark, t), t)
        for t in IMDB_SCHEMAS
    ]
    out = results[0]
    for r in results[1:]:
        out = out.unionByName(r)
    return out


if __name__ == "__main__":
    from pathlib import Path

    from pyspark.sql.functions import desc

    from ..spark_session import get_spark

    out_dir = (
        Path(__file__).resolve().parents[2]
        / "output" / "preparation" / "null_report"
    )
    spark = get_spark()
    report = run(spark).orderBy(desc("null_pct"))

    print("Null percentage per column (all tables):")
    report.show(100, truncate=False)

    print("Top 5 columns with the highest null percentage:")
    report.limit(5).show(truncate=False)

    report.coalesce(1).write.mode("overwrite").option("header", True).csv(str(out_dir))
    print(f"CSV written to {out_dir}")
