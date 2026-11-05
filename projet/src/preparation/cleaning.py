"""Data cleaning and anomaly handling for the IMDb dataset.

Strategy: keep all raw rows but add a `_quality_flags` column listing
the real inconsistencies of each row. Legitimately missing values
(e.g. no deathYear for a living person) are not flagged.
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import array, col, filter as array_filter, lit, when

from ..io_utils import read_imdb


def _flags(*conditions):
    """Build a flags array from (condition, label) pairs, dropping nulls."""
    raw = array(*[when(cond, lit(label)) for cond, label in conditions])
    return array_filter(raw, lambda x: x.isNotNull())


def clean_title_basics(df: DataFrame) -> DataFrame:
    start = col("startYear").cast("int")
    end = col("endYear").cast("int")
    runtime = col("runtimeMinutes").cast("int")
    return (
        df.filter(col("tconst").isNotNull())
        .withColumn(
            "_quality_flags",
            _flags(
                ((start < 1874) | (start > 2030), "invalid_start_year"),
                (end < start, "end_before_start"),
                (runtime <= 0, "non_positive_runtime"),
            ),
        )
    )


def clean_name_basics(df: DataFrame) -> DataFrame:
    birth = col("birthYear").cast("int")
    death = col("deathYear").cast("int")
    return (
        df.filter(col("nconst").isNotNull())
        .withColumn(
            "_quality_flags",
            _flags((death < birth, "death_before_birth")),
        )
    )


def clean_title_ratings(df: DataFrame) -> DataFrame:
    rating = col("averageRating").cast("double")
    votes = col("numVotes").cast("int")
    return (
        df.filter(col("tconst").isNotNull())
        .withColumn(
            "_quality_flags",
            _flags(
                ((rating < 0) | (rating > 10), "rating_out_of_bounds"),
                (votes < 0, "negative_votes"),
            ),
        )
    )


def clean_title_principals(df: DataFrame, names: DataFrame) -> DataFrame:
    known = (
        names.select(col("nconst").alias("_known_nconst"))
        .distinct()
        .withColumn("_known", lit(True))
    )
    return (
        df.filter(col("tconst").isNotNull() & col("nconst").isNotNull())
        .join(known, col("nconst") == col("_known_nconst"), "left")
        .withColumn(
            "_quality_flags",
            _flags((col("_known").isNull(), "orphan_nconst")),
        )
        .drop("_known_nconst", "_known")
    )


def run(spark: SparkSession) -> dict:
    return {
        "title.basics": clean_title_basics(read_imdb(spark, "title.basics")),
        "name.basics": clean_name_basics(read_imdb(spark, "name.basics")),
        "title.ratings": clean_title_ratings(read_imdb(spark, "title.ratings")),
        "title.principals": clean_title_principals(
            read_imdb(spark, "title.principals"),
            read_imdb(spark, "name.basics"),
        ),
    }


if __name__ == "__main__":
    from pathlib import Path

    from ..spark_session import get_spark

    out_dir = Path(__file__).resolve().parents[2] / "data" / "processed"
    spark = get_spark()
    for name, df in run(spark).items():
        df.write.mode("overwrite").parquet(str(out_dir / name.replace(".", "_")))
        print(f"written: {name}")
