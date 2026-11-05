"""Data quality checks for the IMDb dataset (Q1.3)."""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col

from ..io_utils import read_imdb


def check_death_before_birth(spark: SparkSession) -> DataFrame:
    df = read_imdb(spark, "name.basics")
    n = df.filter(
        col("birthYear").isNotNull()
        & col("deathYear").isNotNull()
        & (col("deathYear") < col("birthYear"))
    ).count()
    return spark.createDataFrame(
        [("deathYear < birthYear", n)], ["issue", "count"]
    )


def check_invalid_years(spark: SparkSession) -> DataFrame:
    df = read_imdb(spark, "title.basics")
    n = df.filter(
        col("startYear").isNotNull()
        & ((col("startYear") < 1874) | (col("startYear") > 2030))
    ).count()
    return spark.createDataFrame(
        [("title.basics.startYear out of [1874, 2030]", n)], ["issue", "count"]
    )


def check_negative_runtime(spark: SparkSession) -> DataFrame:
    df = read_imdb(spark, "title.basics")
    n = df.filter(col("runtimeMinutes") < 0).count()
    return spark.createDataFrame(
        [("runtimeMinutes < 0", n)], ["issue", "count"]
    )


def check_ratings_out_of_bounds(spark: SparkSession) -> DataFrame:
    df = read_imdb(spark, "title.ratings")
    n = df.filter(
        (col("averageRating") < 0)
        | (col("averageRating") > 10)
        | (col("numVotes") < 0)
    ).count()
    return spark.createDataFrame(
        [("averageRating not in [0,10] or numVotes < 0", n)], ["issue", "count"]
    )


def check_orphan_principals_nconst(spark: SparkSession) -> DataFrame:
    principals = read_imdb(spark, "title.principals").select("nconst").distinct()
    names = read_imdb(spark, "name.basics").select("nconst").distinct()
    n = principals.join(names, "nconst", "left_anti").count()
    return spark.createDataFrame(
        [("principals.nconst missing in name.basics", n)], ["issue", "count"]
    )


def check_orphan_principals_tconst(spark: SparkSession) -> DataFrame:
    principals = read_imdb(spark, "title.principals").select("tconst").distinct()
    titles = read_imdb(spark, "title.basics").select("tconst").distinct()
    n = principals.join(titles, "tconst", "left_anti").count()
    return spark.createDataFrame(
        [("principals.tconst missing in title.basics", n)], ["issue", "count"]
    )


def check_orphan_ratings(spark: SparkSession) -> DataFrame:
    ratings = read_imdb(spark, "title.ratings").select("tconst").distinct()
    titles = read_imdb(spark, "title.basics").select("tconst").distinct()
    n = ratings.join(titles, "tconst", "left_anti").count()
    return spark.createDataFrame(
        [("ratings.tconst missing in title.basics", n)], ["issue", "count"]
    )


def check_orphan_episodes(spark: SparkSession) -> DataFrame:
    episodes = read_imdb(spark, "title.episode") \
        .select("parentTconst") \
        .withColumnRenamed("parentTconst", "tconst") \
        .distinct()
    titles = read_imdb(spark, "title.basics").select("tconst").distinct()
    n = episodes.join(titles, "tconst", "left_anti").count()
    return spark.createDataFrame(
        [("episode.parentTconst missing in title.basics", n)], ["issue", "count"]
    )


def run(spark: SparkSession) -> DataFrame:
    checks = [
        check_death_before_birth,
        check_invalid_years,
        check_negative_runtime,
        check_ratings_out_of_bounds,
        check_orphan_principals_nconst,
        check_orphan_principals_tconst,
        check_orphan_ratings,
        check_orphan_episodes,
    ]
    results = [fn(spark) for fn in checks]
    out = results[0]
    for r in results[1:]:
        out = out.unionByName(r)
    return out


if __name__ == "__main__":
    from ..spark_session import get_spark

    spark = get_spark()
    run(spark).show(truncate=False)
