"""Q1-Q3: runtime outliers per genre, top-3 genres per year, top-3 vs rest."""
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from ..spark_session import get_spark
from .common import load_clean, save_csv


def explode_genres(basics: DataFrame) -> DataFrame:
    return basics.withColumn("genre", F.explode(F.split("genres", ",")))


# Q1 ---------------------------------------------------------------
def runtime_outliers(basics: DataFrame, z_threshold: float = 2.0) -> DataFrame:
    """Feature-length movies whose runtime is far from their genre mean.

    Feature-length = titleType 'movie' and runtime >= 40 min.
    Atypical = |z-score| > z_threshold within the genre.
    A multi-genre movie is evaluated in each of its genres.
    """
    movies = explode_genres(
        basics.filter((F.col("titleType") == "movie")
                      & (F.col("runtimeMinutes") >= 40)
                      & F.col("genres").isNotNull())
    )
    stats = movies.groupBy("genre").agg(
        F.avg("runtimeMinutes").alias("avg_runtime"),
        F.stddev("runtimeMinutes").alias("std_runtime"),
        F.count("*").alias("n_movies"),
    )
    return (
        movies.join(stats, "genre")
        .withColumn("z_score",
                    (F.col("runtimeMinutes") - F.col("avg_runtime"))
                    / F.col("std_runtime"))
        .filter(F.abs("z_score") > z_threshold)
        .select("genre", "tconst", "primaryTitle", "startYear",
                "runtimeMinutes", "avg_runtime", "std_runtime", "z_score")
    )


# Q2 ---------------------------------------------------------------
def genre_year_stats(basics: DataFrame, ratings: DataFrame) -> DataFrame:
    """Per (year, genre): movie count, total votes, mean rating.

    Popularity = total number of votes (audience size), not production volume.
    """
    movies = explode_genres(
        basics.filter((F.col("titleType") == "movie")
                      & F.col("startYear").between(1980, 2025)
                      & F.col("genres").isNotNull())
    ).join(ratings, "tconst")
    return movies.groupBy("startYear", "genre").agg(
        F.count("*").alias("n_movies"),
        F.sum("numVotes").alias("total_votes"),
        F.avg("averageRating").alias("avg_rating"),
    )


def rank_genres(stats: DataFrame) -> DataFrame:
    w = Window.partitionBy("startYear").orderBy(F.desc("total_votes"))
    return stats.withColumn("rank", F.row_number().over(w))


def top3_per_year(ranked: DataFrame) -> DataFrame:
    return (ranked.filter(F.col("rank") <= 3)
                  .orderBy("startYear", "rank"))


# Q3 ---------------------------------------------------------------
def top3_vs_rest(ranked: DataFrame) -> DataFrame:
    grouped = ranked.withColumn(
        "grp", F.when(F.col("rank") <= 3, "top3").otherwise("rest"))
    return (
        grouped.groupBy("startYear", "grp").agg(
            F.avg("avg_rating").alias("avg_rating"),
            F.avg("n_movies").alias("avg_movies_per_genre"),
            F.avg("total_votes").alias("avg_votes_per_genre"),
        )
        .orderBy("startYear", "grp")
    )


def main() -> None:
    spark = get_spark("Exploration-Genres")
    spark.sparkContext.setLogLevel("ERROR")
    basics = load_clean(spark, "title_basics")
    ratings = load_clean(spark, "title_ratings")

    q1 = runtime_outliers(basics).cache()
    print("Q1 - outliers per genre:")
    q1.groupBy("genre").count().orderBy(F.desc("count")).show(30, False)
    save_csv(q1, "q1_runtime_outliers")

    ranked = rank_genres(genre_year_stats(basics, ratings)).cache()
    q2 = top3_per_year(ranked)
    print("Q2 - top 3 genres per year:")
    q2.show(30, False)
    save_csv(q2, "q2_top3_genres_per_year")

    q3 = top3_vs_rest(ranked)
    print("Q3 - top 3 vs rest:")
    q3.show(30, False)
    save_csv(q3, "q3_top3_vs_rest")


if __name__ == "__main__":
    main()
