from pyspark.sql import functions as F
from pyspark.sql import Window

from projet.src.config import OUTPUT_DIR
from projet.src.io_utils import read_imdb
from projet.src.spark_session import get_spark


def main():
    spark = get_spark("series_analysis")

    episodes = read_imdb(spark, "title.episode")
    ratings = read_imdb(spark, "title.ratings")
    basics = read_imdb(spark, "title.basics")

    # Only episodes with both season and episode numbers, and a rating
    episodes = episodes.dropna(subset=["parentTconst", "seasonNumber", "episodeNumber"])
    ratings = ratings.dropna(subset=["averageRating"])

    tv_series = (
        basics
        .filter(F.col("titleType") == "tvSeries")
        .select("tconst", "primaryTitle")
    )

    ep_ratings = (
        episodes
        .join(ratings, "tconst", "inner")
        .join(tv_series, episodes.parentTconst == tv_series.tconst, "inner")
        .select(
            F.col("parentTconst").alias("series_id"),
            "primaryTitle",
            F.col("seasonNumber").alias("season"),
            F.col("episodeNumber").alias("episode"),
            F.col("averageRating").alias("rating"),
        )
    )

    # Condition 1: >=3 seasons, each with >=10 episodes
    season_counts = (
        ep_ratings
        .groupBy("series_id", "season")
        .agg(F.count("episode").alias("nb_episodes"))
        .filter(F.col("nb_episodes") >= 10)
    )
    valid_series = (
        season_counts
        .groupBy("series_id")
        .agg(F.count("season").alias("nb_valid_seasons"))
        .filter(F.col("nb_valid_seasons") >= 3)
        .select("series_id")
    )

    # Best and worst episode per series (deterministic tie-break)
    w_best = Window.partitionBy("series_id").orderBy(
        F.desc("rating"), F.asc("season"), F.asc("episode")
    )
    w_worst = Window.partitionBy("series_id").orderBy(
        F.asc("rating"), F.asc("season"), F.asc("episode")
    )

    best_ep = (
        ep_ratings
        .withColumn("rn", F.row_number().over(w_best))
        .filter(F.col("rn") == 1)
        .select(
            "series_id",
            F.col("season").alias("best_season"),
            F.col("rating").alias("best_rating"),
        )
    )
    worst_ep = (
        ep_ratings
        .withColumn("rn", F.row_number().over(w_worst))
        .filter(F.col("rn") == 1)
        .select(
            "series_id",
            F.col("season").alias("worst_season"),
            F.col("rating").alias("worst_rating"),
        )
    )
    last_season = (
        ep_ratings
        .groupBy("series_id")
        .agg(F.max("season").alias("last_season"))
    )

    # Final filter
    result = (
        valid_series
        .join(best_ep, "series_id")
        .join(worst_ep, "series_id")
        .join(last_season, "series_id")
        .join(tv_series, valid_series.series_id == tv_series.tconst, "left")
        .filter(
            (F.col("worst_season") == F.col("last_season"))
            & (F.col("best_season") <= 2)
        )
        .select(
            "series_id",
            "primaryTitle",
            "best_season",
            "best_rating",
            "worst_season",
            "worst_rating",
            "last_season",
        )
        .orderBy("primaryTitle")
    )

    result.show(50, truncate=False)
    print(f"Total matching series: {result.count()}")

    out_dir = OUTPUT_DIR.parent / "exploration" / "q5_tv_series"
    result.coalesce(1).write.mode("overwrite").option("header", True).csv(str(out_dir))
    print(f"Written to {out_dir}")


if __name__ == "__main__":
    main()
