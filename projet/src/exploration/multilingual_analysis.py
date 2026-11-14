"""Q6: actors/actresses with successful movies in several original languages."""
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from ..io_utils import read_imdb
from ..spark_session import get_spark
from .common import load_clean, save_csv

# Used only when a title row has a region but no language.
# Ambiguous regions (IN, CN, CH, BE, CA...) are left out on purpose.
REGION_TO_LANG = {
    "US": "en", "GB": "en", "AU": "en", "FR": "fr", "DE": "de",
    "ES": "es", "MX": "es", "AR": "es", "IT": "it", "JP": "ja",
    "KR": "ko", "RU": "ru", "BR": "pt", "PT": "pt", "TR": "tr",
    "SE": "sv", "DK": "da", "NO": "no", "FI": "fi", "NL": "nl",
    "PL": "pl", "GR": "el", "HU": "hu", "CZ": "cs", "IR": "fa",
}


def original_languages(akas: DataFrame, basics: DataFrame,
                       movies: DataFrame) -> DataFrame:
    """One (tconst, lang) row per movie whose original language can be
    inferred. Only the movies in `movies` are processed.

    IMDb has no original-language column, and the 'original title' rows of
    title.akas carry neither language nor region. The language is therefore
    inferred in two steps:
    1. If an original-title row has a language (or a mappable region), use it.
    2. Otherwise, look at the aliases that are identical to the original
       title: each such alias has a language (or a mappable region). The
       language shared by the largest number of distinct regions wins;
       ties are discarded as ambiguous.
    """
    mapping = F.create_map(
        *[F.lit(x) for kv in REGION_TO_LANG.items() for x in kv])
    lang_expr = F.coalesce(F.col("language"), mapping[F.col("region")])

    wanted = F.broadcast(movies.select(F.col("tconst").alias("titleId")))
    rows = akas.join(wanted, "titleId")
    titles = basics.select(F.col("tconst").alias("titleId"), "originalTitle")

    # 1. Strong signal: the original-title row itself.
    strong = (
        rows.filter(F.col("isOriginalTitle") == 1)
        .select(F.col("titleId").alias("tconst"), lang_expr.alias("lang"))
        .filter(F.col("lang").isNotNull())
        .groupBy("tconst")
        .agg(F.countDistinct("lang").alias("n"), F.first("lang").alias("lang"))
        .filter(F.col("n") == 1)
        .select("tconst", F.col("lang").alias("strong_lang"))
    )

    # 2. Weak signal: majority vote among aliases equal to the original title.
    same = (
        rows.join(titles, "titleId")
        .filter(F.col("title") == F.col("originalTitle"))
        .select("titleId", "region", lang_expr.alias("lang"))
        .filter(F.col("lang").isNotNull() & F.col("region").isNotNull())
    )
    votes = same.groupBy("titleId", "lang").agg(
        F.countDistinct("region").alias("n_regions"))
    w = Window.partitionBy("titleId").orderBy(F.desc("n_regions"))
    ranked = (votes.withColumn("rk", F.row_number().over(w))
              .withColumn("runner_up", F.lead("n_regions").over(w)))
    weak = (
        ranked.filter((F.col("rk") == 1)
                      & (F.col("runner_up").isNull()
                         | (F.col("runner_up") < F.col("n_regions"))))
        .select(F.col("titleId").alias("tconst"),
                F.col("lang").alias("weak_lang"))
    )

    return (
        strong.join(weak, "tconst", "full_outer")
        .select("tconst",
                F.coalesce("strong_lang", "weak_lang").alias("lang"))
    )


def successful_movies(basics: DataFrame, ratings: DataFrame,
                      min_rating: float, min_votes: int) -> DataFrame:
    movies = basics.filter(F.col("titleType") == "movie").select("tconst")
    good = ratings.filter((F.col("averageRating") >= min_rating)
                          & (F.col("numVotes") >= min_votes)).select("tconst")
    return movies.join(good, "tconst")


def actor_languages(principals: DataFrame, movies_langs: DataFrame,
                    names: DataFrame, max_ordering: int = 5) -> DataFrame:
    """Per actor: number of distinct languages of successful movies where
    he or she has a leading role (ordering <= max_ordering)."""
    actors = (principals
              .filter(F.col("category").isin("actor", "actress")
                      & (F.col("ordering") <= max_ordering))
              .select("tconst", "nconst").distinct())
    return (
        actors.join(movies_langs, "tconst")
        .groupBy("nconst").agg(
            F.countDistinct("lang").alias("nb_langs"),
            F.countDistinct("tconst").alias("nb_movies"),
            F.array_join(F.array_sort(F.collect_set("lang")), ", ")
             .alias("languages"))
        .join(names.select("nconst", "primaryName"), "nconst")
        .select("nconst", "primaryName", "nb_langs", "languages", "nb_movies")
    )


def main(min_rating: float = 7.0, min_votes: int = 10000) -> None:
    spark = get_spark("Exploration-Languages")
    spark.sparkContext.setLogLevel("ERROR")
    basics = load_clean(spark, "title_basics")
    ratings = load_clean(spark, "title_ratings")
    names = load_clean(spark, "name_basics")
    principals = load_clean(spark, "title_principals", drop_flagged=False)
    akas = read_imdb(spark, "title.akas")

    good = successful_movies(basics, ratings, min_rating, min_votes).cache()
    langs = original_languages(akas, basics, good)
    movies_langs = good.join(langs, "tconst").cache()

    print(f"Successful movies (rating >= {min_rating}, votes >= {min_votes}): "
          f"{good.count()}")
    print(f"... of which with an inferred original language: "
          f"{movies_langs.count()}")
    movies_langs.groupBy("lang").count().orderBy(F.desc("count")).show(15)

    result = actor_languages(principals, movies_langs, names).cache()
    multi = result.filter(F.col("nb_langs") >= 2).orderBy(
        F.desc("nb_langs"), F.desc("nb_movies"))
    three = multi.filter(F.col("nb_langs") >= 3)

    print(f"Q6a - actors with success in >= 2 languages: {multi.count()}")
    multi.show(20, False)
    print(f"Q6b - actors with success in >= 3 languages: {three.count()}")
    three.show(20, False)

    save_csv(multi, "q6_multilingual_2plus_languages")
    save_csv(three, "q6_multilingual_3plus_languages")


if __name__ == "__main__":
    main()
