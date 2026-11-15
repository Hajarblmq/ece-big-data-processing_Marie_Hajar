"""Q7: actors/actresses with a career of 20+ years and 15+ movies whose
popularity peaked early in their career."""
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from ..spark_session import get_spark
from .common import load_clean, save_csv


def actor_movies(principals: DataFrame, basics: DataFrame,
                 ratings: DataFrame, min_votes: int) -> DataFrame:
    """One row per (actor, movie) for rated movies with a leading role.

    Leading role = actor or actress with ordering <= 5. Only movies with a
    release year and at least `min_votes` votes are kept.
    """
    movies = (
        basics.filter((F.col("titleType") == "movie")
                      & F.col("startYear").isNotNull())
        .select("tconst", "startYear")
        .join(ratings.filter(F.col("numVotes") >= min_votes)
              .select("tconst", "averageRating", "numVotes"), "tconst")
    )
    actors = (principals
              .filter(F.col("category").isin("actor", "actress")
                      & (F.col("ordering") <= 5))
              .select("nconst", "tconst").distinct())
    return actors.join(F.broadcast(movies), "tconst")


def career_halves(movies: DataFrame, min_movies: int = 15,
                  min_span: int = 20) -> DataFrame:
    """Per actor with >= `min_movies` movies over >= `min_span` years: mean
    and best rating of the first half and of the second half of the career.

    The movies of an actor are sorted by year (then by id, so that the order is
    reproducible) and cut in two halves of equal size.
    """
    by_actor = Window.partitionBy("nconst")
    ordered = Window.partitionBy("nconst").orderBy("startYear", "tconst")
    ranked = (
        movies
        .withColumn("n_movies", F.count("*").over(by_actor))
        .withColumn("first_year", F.min("startYear").over(by_actor))
        .withColumn("last_year", F.max("startYear").over(by_actor))
        .filter((F.col("n_movies") >= min_movies)
                & (F.col("last_year") - F.col("first_year") >= min_span))
        .withColumn("rank", F.row_number().over(ordered))
        .withColumn("early", F.col("rank") <= F.ceil(F.col("n_movies") / 2))
    )
    early = F.col("early")
    return ranked.groupBy("nconst", "n_movies", "first_year", "last_year").agg(
        F.avg(F.when(early, F.col("averageRating"))).alias("avg_early"),
        F.avg(F.when(~early, F.col("averageRating"))).alias("avg_late"),
        F.max(F.when(early, F.col("averageRating"))).alias("best_early"),
        F.max(F.when(~early, F.col("averageRating"))).alias("best_late"),
    )


def early_peak(halves: DataFrame, names: DataFrame,
               min_drop: float = 0.5) -> DataFrame:
    """Actors whose best movie is in the first half of the career and whose
    mean rating drops by at least `min_drop` points in the second half."""
    return (
        halves
        .withColumn("drop", F.col("avg_early") - F.col("avg_late"))
        .filter((F.col("drop") >= min_drop)
                & (F.col("best_early") > F.col("best_late")))
        .join(names.select("nconst", "primaryName"), "nconst")
        .select("nconst", "primaryName", "first_year", "last_year",
                "n_movies",
                F.round("avg_early", 2).alias("avg_early"),
                F.round("avg_late", 2).alias("avg_late"),
                F.round("drop", 2).alias("drop"),
                F.col("best_early"), F.col("best_late"))
        .orderBy(F.desc("drop"), F.desc("n_movies"))
    )


def main(min_votes: int = 1000, min_drop: float = 0.5) -> None:
    spark = get_spark("Exploration-Careers")
    spark.sparkContext.setLogLevel("ERROR")
    basics = load_clean(spark, "title_basics")
    ratings = load_clean(spark, "title_ratings")
    names = load_clean(spark, "name_basics")
    principals = load_clean(spark, "title_principals", drop_flagged=False)

    movies = actor_movies(principals, basics, ratings, min_votes)
    halves = career_halves(movies).cache()
    result = early_peak(halves, names, min_drop).cache()

    n_valid = halves.count()
    n_naive = halves.filter(F.col("avg_early") > F.col("avg_late")).count()
    n_result = result.count()
    print(f"Actors with >= 15 movies (>= {min_votes} votes) over >= 20 years: "
          f"{n_valid}")
    print(f"... with a higher mean rating in the first half (no margin): "
          f"{n_naive}")
    print(f"... early peak (best movie in the first half and drop >= "
          f"{min_drop}): {n_result}")
    result.show(25, False)

    save_csv(result, "q7_early_peaks")


if __name__ == "__main__":
    main()
