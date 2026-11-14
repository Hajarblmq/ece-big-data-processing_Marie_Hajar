"""Step 1 (Spark): choose which films and people to enrich.

For every (year, genre) pair, the top N feature-length films by number
of IMDb votes are kept, together with the first K credited principals
of each film. Nothing is hard-coded to the restricted scope: the year
range, N and K are parameters, so the same code runs on the full
dataset by widening them.

Run from the `projet` folder:
    python -m src.enrichment.select_targets
"""
import argparse

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from ..exploration.common import load_clean
from .common import IDS_DIR, TARGETS_DIR

FEATURE_MIN_RUNTIME = 40  # minutes (AFI / Academy definition)


def feature_films(spark: SparkSession, first_year: int,
                  last_year: int) -> DataFrame:
    """Rated feature-length movies, one row per (film, genre).

    Adult titles (`isAdult = 1`, genre "Adult") are excluded: they are
    out of scope for a public encyclopedic enrichment and almost never
    have a Wikipedia article.
    """
    basics = load_clean(spark, "title_basics").filter(
        (F.col("titleType") == "movie")
        & (F.col("isAdult") == 0)
        & (F.col("runtimeMinutes") >= FEATURE_MIN_RUNTIME)
        & F.col("startYear").between(first_year, last_year)
    )
    ratings = load_clean(spark, "title_ratings")
    return (
        basics.join(ratings, "tconst")
        .withColumn("genre", F.explode(F.split("genres", ",")))
        .filter(F.col("genre").isNotNull() & (F.col("genre") != ""))
        .select(
            "tconst", "primaryTitle", "originalTitle", "startYear",
            "runtimeMinutes", "genre", "averageRating", "numVotes",
        )
    )


def top_films(films: DataFrame, top_n: int) -> DataFrame:
    """Top `top_n` films per (year, genre), ranked by number of votes."""
    window = Window.partitionBy("startYear", "genre").orderBy(
        F.desc("numVotes"), F.desc("averageRating"), "tconst"
    )
    return (
        films.withColumn("rank_in_genre", F.row_number().over(window))
        .filter(F.col("rank_in_genre") <= top_n)
    )


def top_principals(spark: SparkSession, film_ids: DataFrame,
                   per_film: int) -> DataFrame:
    """First `per_film` credited people of each film (IMDb ordering)."""
    principals = load_clean(spark, "title_principals")
    names = load_clean(spark, "name_basics").select(
        "nconst", "primaryName", "birthYear", "deathYear",
        "primaryProfession",
    )
    window = Window.partitionBy("tconst").orderBy("ordering", "nconst")
    return (
        principals.join(film_ids, "tconst")
        .join(names, "nconst")  # drops people missing from name.basics
        .withColumn("principal_rank", F.row_number().over(window))
        .filter(F.col("principal_rank") <= per_film)
        .select(
            "tconst", "nconst", "ordering", "category", "primaryName",
            "birthYear", "deathYear", "primaryProfession",
        )
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--first-year", type=int, default=1980)
    parser.add_argument("--last-year", type=int, default=2025)
    parser.add_argument("--top-n", type=int, default=10)
    parser.add_argument("--principals", type=int, default=3)
    args = parser.parse_args()

    from ..spark_session import get_spark

    spark = get_spark("enrichment-targets")
    spark.sparkContext.setLogLevel("ERROR")

    films = top_films(
        feature_films(spark, args.first_year, args.last_year), args.top_n
    )
    films.write.mode("overwrite").parquet(str(TARGETS_DIR / "films"))
    films = spark.read.parquet(str(TARGETS_DIR / "films"))

    film_ids = films.select("tconst").distinct()
    people = top_principals(spark, film_ids, args.principals)
    people.write.mode("overwrite").parquet(str(TARGETS_DIR / "principals"))
    people = spark.read.parquet(str(TARGETS_DIR / "principals"))

    ids = (
        film_ids.select(F.col("tconst").alias("imdb_id"),
                        F.lit("film").alias("kind"))
        .unionByName(
            people.select(F.col("nconst").alias("imdb_id"),
                          F.lit("person").alias("kind")).distinct()
        )
    )
    (ids.coalesce(1).write.mode("overwrite").option("header", True)
        .csv(str(IDS_DIR)))

    print(f"(year, genre, film) slots : {films.count()}")
    print(f"distinct films            : {film_ids.count()}")
    print(f"(film, principal) pairs   : {people.count()}")
    print(f"distinct people           : "
          f"{people.select('nconst').distinct().count()}")
    films.filter(F.col("startYear") == args.last_year).orderBy(
        "genre", "rank_in_genre"
    ).show(12, truncate=40)
    spark.stop()


if __name__ == "__main__":
    main()
