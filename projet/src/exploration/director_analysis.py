"""Q4: directors with exactly one movie rated >= 8.0 and several other
movies whose average rating is <= 6.0."""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from ..spark_session import get_spark
from .common import load_clean, save_csv


def directed_movies(basics: DataFrame, ratings: DataFrame,
                    principals: DataFrame, min_votes: int) -> DataFrame:
    """One row per (director, movie) with the movie rating.

    Only movies (titleType = 'movie') with at least `min_votes` votes are
    kept, so that ratings based on a handful of votes do not count.
    """
    movies = basics.filter(F.col("titleType") == "movie").select("tconst")
    rated = ratings.filter(F.col("numVotes") >= min_votes)
    return (
        principals.filter(F.col("category") == "director")
        .select("nconst", "tconst").distinct()
        .join(movies, "tconst")
        .join(rated.select("tconst", "averageRating"), "tconst")
    )


def one_hit_directors(directed: DataFrame, names: DataFrame,
                      high: float = 8.0, low_avg: float = 6.0,
                      min_others: int = 2) -> DataFrame:
    """Directors with exactly one movie >= `high`, and at least
    `min_others` other movies whose mean rating is <= `low_avg`.

    "Other movies" = every movie of the director except the one >= `high`.
    """
    per_director = directed.groupBy("nconst").agg(
        F.count("*").alias("n_movies"),
        F.sum(F.when(F.col("averageRating") >= high, 1).otherwise(0))
         .alias("n_high"),
        F.sum("averageRating").alias("sum_all"),
        F.max("averageRating").alias("max_rating"),
    )
    return (
        per_director.filter(F.col("n_high") == 1)
        .withColumn("n_others", F.col("n_movies") - 1)
        .filter(F.col("n_others") >= min_others)
        # exactly one movie >= high, so it is the max
        .withColumn("avg_others",
                    (F.col("sum_all") - F.col("max_rating"))
                    / F.col("n_others"))
        .filter(F.col("avg_others") <= low_avg)
        .join(names.select("nconst", "primaryName"), "nconst")
        .select("nconst", "primaryName",
                F.col("max_rating").alias("best_rating"),
                "n_others", "avg_others")
        .orderBy(F.desc("best_rating"), "avg_others")
    )


def main(min_votes: int = 1000) -> None:
    spark = get_spark("Exploration-Directors")
    spark.sparkContext.setLogLevel("ERROR")
    basics = load_clean(spark, "title_basics")
    ratings = load_clean(spark, "title_ratings")
    names = load_clean(spark, "name_basics")
    principals = load_clean(spark, "title_principals", drop_flagged=False)

    directed = directed_movies(basics, ratings, principals, min_votes)
    result = one_hit_directors(directed, names).cache()

    print(f"Q4 - directors found (min_votes={min_votes}): {result.count()}")
    result.show(30, False)
    save_csv(result, "q4_one_hit_directors")


if __name__ == "__main__":
    main()
