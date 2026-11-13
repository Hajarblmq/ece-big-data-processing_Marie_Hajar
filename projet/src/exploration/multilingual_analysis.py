"""Q6: actors/actresses with successful movies in several original languages."""
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from projet.src.io_utils import read_imdb
from projet.src.spark_session import get_spark
from projet.src.config import OUTPUT_DIR

# Fallback used only when the original-title row has no language.
REGION_TO_LANG = {
    "US": "en", "GB": "en", "AU": "en", "FR": "fr", "DE": "de",
    "ES": "es", "MX": "es", "AR": "es", "IT": "it", "JP": "ja",
    "KR": "ko", "RU": "ru", "BR": "pt", "PT": "pt", "TR": "tr",
    "SE": "sv", "DK": "da", "NO": "no", "FI": "fi", "NL": "nl",
    "PL": "pl", "GR": "el", "HU": "hu", "CZ": "cs", "IR": "fa",
}


def original_languages(akas: DataFrame) -> DataFrame:
    """One (tconst, lang) row per title, deduced from its non-US/GB regions.
    
    If a movie has regions other than US/GB, we take the language
    of the most frequent non-US/GB region (e.g. FR -> fr).
    Otherwise, we default to 'en'.
    """
    mapping = F.create_map(
        *[F.lit(x) for kv in REGION_TO_LANG.items() for x in kv])
    
    # On prend les langues des régions qui ne sont ni US ni GB
    foreign_candidates = (
        akas.filter(
            (F.col("region").isNotNull()) & 
            (~F.col("region").isin("US", "GB"))
        )
        .withColumn("lang", mapping[F.col("region")])
        .filter(F.col("lang").isNotNull())
        .select("titleId", "lang")
    )
    
    # On garde la langue la plus fréquente pour chaque film
    w = Window.partitionBy("titleId").orderBy(F.desc("count"))
    
    foreign_langs = (
        foreign_candidates.groupBy("titleId", "lang").count()
        .withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") == 1)
        .select(F.col("titleId").alias("tconst"), "lang")
    )
    
    # Pour les films qui n'ont QUE des régions US/GB, on met 'en'
    all_titles = akas.select(F.col("titleId").alias("tconst")).distinct()
    en_titles = all_titles.join(foreign_langs, "tconst", "left_anti") \
        .withColumn("lang", F.lit("en"))
        
    # On fusionne les deux
    return foreign_langs.union(en_titles)


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
    
    basics = read_imdb(spark, "title.basics")
    ratings = read_imdb(spark, "title.ratings")
    names = read_imdb(spark, "name.basics")
    principals = read_imdb(spark, "title.principals")
    akas = read_imdb(spark, "title.akas")

    good = successful_movies(basics, ratings, min_rating, min_votes).cache()
    langs = original_languages(akas)
    movies_langs = good.join(langs, "tconst").cache()

    print(f"Successful movies (rating >= {min_rating}, votes >= {min_votes}): "
          f"{good.count()}")
    print(f"... of which with a known original language: "
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

    # Sauvegarde
    out_dir_2 = OUTPUT_DIR.parent / "exploration" / "q6_multilingual_2plus"
    out_dir_3 = OUTPUT_DIR.parent / "exploration" / "q6_multilingual_3plus"
    
    multi.coalesce(1).write.mode("overwrite").option("header", True).csv(str(out_dir_2))
    three.coalesce(1).write.mode("overwrite").option("header", True).csv(str(out_dir_3))
    print(f"Written to {out_dir_2} and {out_dir_3}")


if __name__ == "__main__":
    main()
