"""Step 3 (Spark): clean the fetched texts and build the enriched tables.

Inputs: the targets chosen by `select_targets` and the raw JSONL files
written by `wikipedia_fetch`. All cleaning, schema enforcement, joins
and lineage are done here, in Spark.

Outputs (Parquet + CSV under output/enrichment/):
- films_enriched : one row per (year, genre, rank) with the synopsis
- people_enriched: one row per (film, principal) with the biography
- coverage       : share of films / people actually enriched
Every row carries `source_url` (exact Wikipedia revision), the Wikidata
item used for the link, the retrieval time and, when the information is
not available, an `enrichment_note` explaining why.

Run from the `projet` folder:
    python -m src.enrichment.enrichment_pipeline
"""
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType

from .common import (
    LINKS_FILE, OUTPUT_DIR, PAGES_FILE, SOURCE_NAME, TARGETS_DIR,
)

LINK_SCHEMA = StructType([
    StructField("imdb_id", StringType()),
    StructField("kind", StringType()),
    StructField("wikidata_item", StringType()),
    StructField("wiki_title", StringType()),
    StructField("link_source", StringType()),
    StructField("retrieved_at", StringType()),
])

PAGE_SCHEMA = StructType([
    StructField("requested_title", StringType()),
    StructField("page_title", StringType()),
    StructField("pageid", LongType()),
    StructField("revid", LongType()),
    StructField("source_url", StringType()),
    StructField("extract", StringType()),
    StructField("retrieved_at", StringType()),
])

# Level-2 section holding the plot on English Wikipedia film articles.
PLOT_SECTION = (
    r"(?s)\n==\s*(?:Plot|Plot summary|Synopsis|Premise|Story|Summary)"
    r"\s*==\s*\n(.*?)(?=\n==[^=]|\z)"
)


def clean_text(column: Column) -> Column:
    """Squeeze blank lines and turn empty texts into nulls."""
    text = F.regexp_replace(column, r"\n{3,}", "\n\n")
    text = F.regexp_replace(text, r"^\s+|\s+$", "")
    return F.when(text == "", F.lit(None)).otherwise(text)


def load_sources(spark: SparkSession) -> DataFrame:
    """IMDb ID -> Wikidata item -> Wikipedia page, latest record only."""
    links = (
        spark.read.schema(LINK_SCHEMA).json(str(LINKS_FILE))
        .dropDuplicates(["imdb_id"])
        .withColumnRenamed("retrieved_at", "link_retrieved_at")
    )
    pages = (
        spark.read.schema(PAGE_SCHEMA).json(str(PAGES_FILE))
        .dropDuplicates(["requested_title"])
    )
    return links.join(
        pages, links.wiki_title == pages.requested_title, "left"
    ).drop("requested_title")


def note(text_col: str, missing_text: str) -> Column:
    """Explain, in plain words, why a value is missing."""
    return (
        F.when(F.col("imdb_id").isNull(),
               "Not looked up yet (run wikipedia_fetch)")
        .when(F.col("wikidata_item").isNull(),
              "No Wikidata item carries this IMDb ID")
        .when(F.col("wiki_title").isNull(),
              "No English Wikipedia article for this Wikidata item")
        .when(F.col("page_title").isNull(),
              "Wikipedia article not retrieved or missing")
        .when(F.col(text_col).isNull(), missing_text)
    )


def lineage(prefix: str = "") -> list:
    return [
        F.lit(SOURCE_NAME).alias(f"{prefix}source"),
        F.col("source_url").alias(f"{prefix}source_url"),
        F.col("wikidata_item").alias(f"{prefix}wikidata_item"),
        F.col("retrieved_at").alias(f"{prefix}retrieved_at"),
    ]


def enrich_films(films: DataFrame, sources: DataFrame) -> DataFrame:
    film_sources = (
        sources.filter(F.col("kind") == "film")
        .withColumn("synopsis",
                    clean_text(F.regexp_extract("extract", PLOT_SECTION, 1)))
    )
    joined = films.join(
        film_sources, films.tconst == film_sources.imdb_id, "left"
    )
    return joined.select(
        "startYear", "genre", "rank_in_genre", "tconst", "primaryTitle",
        "averageRating", "numVotes",
        F.col("page_title").alias("wikipedia_title"),
        "synopsis",
        F.length("synopsis").alias("synopsis_chars"),
        note("synopsis", "Wikipedia article has no Plot/Synopsis section")
        .alias("enrichment_note"),
        *lineage(),
    )


def enrich_people(people: DataFrame, sources: DataFrame) -> DataFrame:
    person_sources = (
        sources.filter(F.col("kind") == "person")
        .withColumn("biography", clean_text(F.col("extract")))
    )
    joined = people.join(
        person_sources, people.nconst == person_sources.imdb_id, "left"
    )
    return joined.select(
        "tconst", "nconst", "ordering", "category", "primaryName",
        "birthYear", "deathYear",
        F.col("page_title").alias("wikipedia_title"),
        "biography",
        F.length("biography").alias("biography_chars"),
        note("biography", "Wikipedia article has an empty introduction")
        .alias("enrichment_note"),
        *lineage(),
    )


def coverage(films: DataFrame, people: DataFrame) -> DataFrame:
    def stats(df: DataFrame, label: str, key: str, text: str) -> DataFrame:
        distinct = df.dropDuplicates([key])
        return distinct.agg(
            F.lit(label).alias("entity"),
            F.count("*").alias("total"),
            F.count("wikipedia_title").alias("with_wikipedia_page"),
            F.count(text).alias("enriched"),
        ).withColumn(
            "enriched_pct",
            F.round(F.col("enriched") / F.col("total") * 100, 1),
        )

    return stats(films, "film (synopsis)", "tconst", "synopsis").unionByName(
        stats(people, "person (biography)", "nconst", "biography")
    )


def write(df: DataFrame, name: str) -> None:
    df.write.mode("overwrite").parquet(str(OUTPUT_DIR / name))
    (df.coalesce(1).write.mode("overwrite").option("header", True)
       .option("escape", '"')
       .csv(str(OUTPUT_DIR / f"{name}_csv")))


def main() -> None:
    from ..spark_session import get_spark

    spark = get_spark("enrichment-pipeline")
    spark.sparkContext.setLogLevel("ERROR")

    films = spark.read.parquet(str(TARGETS_DIR / "films"))
    people = spark.read.parquet(str(TARGETS_DIR / "principals"))
    sources = load_sources(spark).cache()

    films_out = enrich_films(films, sources)
    people_out = enrich_people(people, sources)
    write(films_out, "films_enriched")
    write(people_out, "people_enriched")

    films_out = spark.read.parquet(str(OUTPUT_DIR / "films_enriched"))
    people_out = spark.read.parquet(str(OUTPUT_DIR / "people_enriched"))
    cov = coverage(films_out, people_out)
    (cov.coalesce(1).write.mode("overwrite").option("header", True)
        .csv(str(OUTPUT_DIR / "coverage")))

    print("Coverage:")
    cov.show(truncate=False)
    print("Why information is missing (films):")
    films_out.dropDuplicates(["tconst"]).groupBy("enrichment_note") \
        .count().orderBy(F.desc("count")).show(truncate=False)
    print("Why information is missing (people):")
    people_out.dropDuplicates(["nconst"]).groupBy("enrichment_note") \
        .count().orderBy(F.desc("count")).show(truncate=False)
    print("Sample:")
    films_out.filter(F.col("synopsis").isNotNull()).select(
        "startYear", "genre", "primaryTitle",
        F.substring("synopsis", 1, 80).alias("synopsis_start"), "source_url",
    ).show(5, truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()
