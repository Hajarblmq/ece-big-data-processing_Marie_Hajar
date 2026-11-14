"""Build the static IMDb lookup table used by the streaming job.

The table lists the names that the stream should react to: popular
titles (movies and series) and the top-billed people of very popular
titles. Popularity is measured with IMDb's number of votes so that
obscure titles or people with generic names (e.g. a short film called
"Paris") do not match every edit of unrelated Wikipedia pages.

Run from the `projet` folder:
    python -m src.streaming.build_lookup --min-votes 5000
"""
import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ..exploration.common import load_clean
from .common import LOOKUP_DIR, normalize_title

TITLE_TYPES = ["movie", "tvMovie", "tvSeries", "tvMiniSeries"]
PERSON_CATEGORIES = ["actor", "actress", "director", "writer"]
MIN_KEY_LENGTH = 4


def popular_titles(spark: SparkSession, min_votes: int) -> DataFrame:
    """Titles of the selected types with at least `min_votes` votes."""
    basics = load_clean(spark, "title_basics").filter(
        F.col("titleType").isin(TITLE_TYPES)
    )
    ratings = load_clean(spark, "title_ratings").filter(
        F.col("numVotes") >= min_votes
    )
    return basics.join(ratings, "tconst").select(
        "tconst", "titleType", "primaryTitle", "startYear", "numVotes"
    )


def title_entries(titles: DataFrame) -> DataFrame:
    """One lookup row per normalized title key."""
    return (
        titles.withColumn("match_key", normalize_title(F.col("primaryTitle")))
        .groupBy("match_key")
        .agg(
            F.lit("title").alias("entity_type"),
            F.max_by("primaryTitle", "numVotes").alias("display_name"),
            F.collect_set("tconst").alias("imdb_ids"),
            F.max("numVotes").alias("num_votes"),
        )
    )


def person_entries(spark: SparkSession, titles: DataFrame,
                   person_min_votes: int, top_billing: int) -> DataFrame:
    """Top-billed cast and crew of very popular titles.

    `knownForTitles` is too broad (it includes every crew member of a
    blockbuster), so people are taken from `title.principals` instead:
    only the first `top_billing` credited actors, directors and writers
    of titles with at least `person_min_votes` votes. Names shared by
    several of these people are ambiguous and are dropped.
    """
    very_popular = titles.filter(F.col("numVotes") >= person_min_votes)
    principals = (
        load_clean(spark, "title_principals", drop_flagged=False)
        .filter(F.col("category").isin(PERSON_CATEGORIES))
        .filter(F.col("ordering") <= top_billing)
        .select("tconst", "nconst")
    )
    names = load_clean(spark, "name_basics").select("nconst", "primaryName")
    return (
        principals
        .join(very_popular.select("tconst", "numVotes"), "tconst")
        .join(names, "nconst")
        .withColumn("match_key", normalize_title(F.col("primaryName")))
        .groupBy("match_key")
        .agg(
            F.lit("person").alias("entity_type"),
            F.first("primaryName").alias("display_name"),
            F.collect_set("nconst").alias("imdb_ids"),
            F.max("numVotes").alias("num_votes"),
        )
        .filter(F.size("imdb_ids") == 1)
    )


def build_lookup(spark: SparkSession, min_votes: int,
                 person_min_votes: int, top_billing: int) -> DataFrame:
    titles = popular_titles(spark, min_votes)
    people = person_entries(spark, titles, person_min_votes, top_billing)
    lookup = title_entries(titles).unionByName(people)
    return lookup.filter(F.length("match_key") >= MIN_KEY_LENGTH)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--min-votes", type=int, default=5000,
        help="minimum number of IMDb votes for a title to be watched",
    )
    parser.add_argument(
        "--person-min-votes", type=int, default=50000,
        help="minimum votes of a title for its top-billed people",
    )
    parser.add_argument(
        "--top-billing", type=int, default=4,
        help="number of first credited people kept per title",
    )
    args = parser.parse_args()

    from ..spark_session import get_spark

    spark = get_spark("streaming-lookup")
    lookup = build_lookup(
        spark, args.min_votes, args.person_min_votes, args.top_billing
    )
    lookup.write.mode("overwrite").parquet(str(LOOKUP_DIR))

    written = spark.read.parquet(str(LOOKUP_DIR))
    print(f"Lookup written to {LOOKUP_DIR}")
    written.groupBy("entity_type").count().show()
    written.orderBy(F.desc("num_votes")).show(10, truncate=40)
    spark.stop()


if __name__ == "__main__":
    main()
