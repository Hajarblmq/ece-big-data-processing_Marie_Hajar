"""Open question: do recurring actor-director pairs make better films
together than apart?

For every (actor, director) pair that worked together on at least
`min_shared` feature films, the average rating of their shared films is
compared with the average rating of the films each of them made
WITHOUT the other. The difference is the "collaboration lift".

Tables joined: title.basics, title.ratings, title.principals (twice:
actors and directors) and name.basics.

Run from the `projet` folder:
    python -m src.open_question.analysis
"""
import argparse
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ..exploration.common import load_clean

OUTPUT_DIR = Path(__file__).resolve().parents[2] / "output" / "open_question"
FEATURE_MIN_RUNTIME = 40


def rated_films(spark: SparkSession, min_votes: int) -> DataFrame:
    """Feature films (non-adult) with at least `min_votes` votes."""
    basics = load_clean(spark, "title_basics").filter(
        (F.col("titleType") == "movie")
        & (F.col("isAdult") == 0)
        & (F.col("runtimeMinutes") >= FEATURE_MIN_RUNTIME)
    )
    ratings = load_clean(spark, "title_ratings").filter(
        F.col("numVotes") >= min_votes
    )
    return basics.join(ratings, "tconst").select(
        "tconst", "primaryTitle", "startYear", "averageRating", "numVotes"
    )


def credits(spark: SparkSession, films: DataFrame,
            top_billing: int) -> tuple:
    """(tconst, actor) for top-billed actors and (tconst, director)."""
    principals = load_clean(spark, "title_principals").join(
        films.select("tconst"), "tconst"
    )
    actors = (
        principals.filter(F.col("category").isin("actor", "actress")
                          & (F.col("ordering") <= top_billing))
        .select("tconst", F.col("nconst").alias("actor")).distinct()
    )
    directors = (
        principals.filter(F.col("category") == "director")
        .select("tconst", F.col("nconst").alias("director")).distinct()
    )
    return actors, directors


def recurring_pairs(films: DataFrame, actors: DataFrame,
                    directors: DataFrame, min_shared: int) -> DataFrame:
    """Pairs with at least `min_shared` films together."""
    shared = (
        actors.join(directors, "tconst")
        .filter(F.col("actor") != F.col("director"))  # not self-directed
        .join(films, "tconst")
    )
    return (
        shared.groupBy("actor", "director").agg(
            F.count("*").alias("shared_films"),
            F.avg("averageRating").alias("rating_together"),
            F.avg("numVotes").alias("votes_together"),
            F.min("startYear").alias("first_year"),
            F.max("startYear").alias("last_year"),
        )
        .filter(F.col("shared_films") >= min_shared)
    )


def apart_baseline(pairs: DataFrame, own_films: DataFrame, me: str,
                   other_credits: DataFrame, other: str,
                   films: DataFrame) -> DataFrame:
    """Average rating of `me`'s films made without `other`.

    `own_films` is (tconst, me) and `other_credits` is (tconst, other).
    A left join on (tconst, other) marks the shared films, which are
    then removed.
    """
    marked = other_credits.withColumn("_shared", F.lit(True))
    return (
        pairs.select(me, other)
        .join(own_films, me)
        .join(marked, ["tconst", other], "left")
        .filter(F.col("_shared").isNull())
        .join(films, "tconst")
        .groupBy(me, other).agg(
            F.count("*").alias(f"{me}_films_apart"),
            F.avg("averageRating").alias(f"{me}_rating_apart"),
            F.avg("numVotes").alias(f"{me}_votes_apart"),
        )
    )


def collaboration_lift(films: DataFrame, actors: DataFrame,
                       directors: DataFrame, min_shared: int,
                       min_apart: int) -> DataFrame:
    pairs = recurring_pairs(films, actors, directors, min_shared)
    actor_apart = apart_baseline(pairs, actors, "actor", directors,
                                 "director", films)
    director_apart = apart_baseline(pairs, directors, "director", actors,
                                    "actor", films)
    rating_apart = (F.col("actor_rating_apart")
                    + F.col("director_rating_apart")) / 2
    votes_apart = (F.col("actor_votes_apart")
                   + F.col("director_votes_apart")) / 2
    return (
        pairs.join(actor_apart, ["actor", "director"])
        .join(director_apart, ["actor", "director"])
        .filter((F.col("actor_films_apart") >= min_apart)
                & (F.col("director_films_apart") >= min_apart))
        .withColumn("rating_apart", rating_apart)
        .withColumn("rating_lift",
                    F.col("rating_together") - F.col("rating_apart"))
        .withColumn("votes_ratio",
                    F.col("votes_together") / votes_apart)
    )


def with_names(lift: DataFrame, names: DataFrame) -> DataFrame:
    actor_names = names.select(F.col("nconst").alias("actor"),
                               F.col("primaryName").alias("actor_name"))
    director_names = names.select(
        F.col("nconst").alias("director"),
        F.col("primaryName").alias("director_name"),
    )
    return (
        lift.join(actor_names, "actor").join(director_names, "director")
        .select(
            "actor_name", "director_name", "shared_films",
            "first_year", "last_year",
            F.round("rating_together", 2).alias("rating_together"),
            F.round("actor_rating_apart", 2).alias("actor_rating_apart"),
            F.round("director_rating_apart", 2)
            .alias("director_rating_apart"),
            F.round("rating_lift", 2).alias("rating_lift"),
            F.round("votes_ratio", 2).alias("votes_ratio"),
            "actor_films_apart", "director_films_apart",
        )
    )


def summary_by_bucket(lift: DataFrame) -> DataFrame:
    """Does the lift grow with the number of collaborations?"""
    bucket = (
        F.when(F.col("shared_films") >= 6, "6+")
        .when(F.col("shared_films") >= 4, "4-5")
        .otherwise("3")
    )
    return (
        lift.withColumn("collaborations", bucket)
        .groupBy("collaborations").agg(
            F.count("*").alias("pairs"),
            F.round(F.avg("rating_lift"), 3).alias("avg_rating_lift"),
            F.round(F.expr("percentile(rating_lift, 0.5)"), 3)
            .alias("median_rating_lift"),
            F.round(F.avg((F.col("rating_lift") > 0).cast("int")) * 100, 1)
            .alias("pct_pairs_better_together"),
            F.round(F.expr("percentile(votes_ratio, 0.5)"), 2)
            .alias("median_votes_ratio"),
        )
        .orderBy("collaborations")
    )


def save(df: DataFrame, name: str) -> None:
    (df.coalesce(1).write.mode("overwrite").option("header", True)
       .csv(str(OUTPUT_DIR / name)))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--min-votes", type=int, default=1000)
    parser.add_argument("--min-shared", type=int, default=3)
    parser.add_argument("--min-apart", type=int, default=3)
    parser.add_argument("--top-billing", type=int, default=5)
    args = parser.parse_args()

    from ..spark_session import get_spark

    spark = get_spark("open-question")
    spark.sparkContext.setLogLevel("ERROR")

    films = rated_films(spark, args.min_votes).cache()
    actors, directors = credits(spark, films, args.top_billing)
    lift = collaboration_lift(films, actors, directors, args.min_shared,
                              args.min_apart).cache()
    names = load_clean(spark, "name_basics")
    pairs = with_names(lift, names).cache()
    buckets = summary_by_bucket(lift)
    overall = lift.agg(
        F.count("*").alias("pairs"),
        F.round(F.avg("rating_lift"), 3).alias("avg_rating_lift"),
        F.round(F.expr("percentile(rating_lift, 0.5)"), 3)
        .alias("median_rating_lift"),
        F.round(F.avg((F.col("rating_lift") > 0).cast("int")) * 100, 1)
        .alias("pct_pairs_better_together"),
        F.round(F.expr("percentile(votes_ratio, 0.5)"), 2)
        .alias("median_votes_ratio"),
    )

    print(f"Films used (>= {args.min_votes} votes): {films.count()}")
    print("Overall:")
    overall.show(truncate=False)
    print("By number of collaborations:")
    buckets.show(truncate=False)
    print("Most frequent pairs:")
    pairs.orderBy(F.desc("shared_films"), F.desc("rating_lift")) \
        .show(15, truncate=25)
    print("Largest positive lift:")
    pairs.orderBy(F.desc("rating_lift")).show(10, truncate=25)
    print("Largest negative lift:")
    pairs.orderBy("rating_lift").show(10, truncate=25)

    save(overall, "overall")
    save(buckets, "lift_by_collaborations")
    save(pairs.orderBy(F.desc("shared_films"), F.desc("rating_lift")),
         "recurring_pairs")
    spark.stop()


if __name__ == "__main__":
    main()
