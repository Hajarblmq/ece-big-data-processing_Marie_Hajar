"""Q7: Actors/actresses with 20+ years career and early popularity peak."""
from pyspark.sql import Window
from pyspark.sql import functions as F

from projet.src.io_utils import read_imdb
from projet.src.spark_session import get_spark
from projet.src.config import OUTPUT_DIR


def main():
    spark = get_spark("Exploration-Careers")
    spark.sparkContext.setLogLevel("ERROR")

    # 1. Chargement des tables
    basics = read_imdb(spark, "title.basics")
    ratings = read_imdb(spark, "title.ratings")
    principals = read_imdb(spark, "title.principals")
    names = read_imdb(spark, "name.basics")

    # 2. On garde uniquement les films avec une note et une année de sortie
    movies = basics.filter(
        (F.col("titleType") == "movie") & 
        F.col("startYear").isNotNull()
    ).select("tconst", "startYear")

    rated_movies = movies.join(ratings, "tconst").select("tconst", "startYear", "averageRating")

    # 3. On garde les acteurs (rôles principaux)
    actors = principals.filter(
        F.col("category").isin("actor", "actress") & (F.col("ordering") <= 5)
    ).select("tconst", "nconst").distinct()

    # 4. Jointure acteurs + films
    actor_movies = actors.join(rated_movies, "tconst")

    # 5. Statistiques par acteur (nombre de films, première et dernière année)
    actor_stats = actor_movies.groupBy("nconst").agg(
        F.count("tconst").alias("nb_movies"),
        F.min("startYear").alias("first_year"),
        F.max("startYear").alias("last_year")
    )

    # Condition 1 : >= 15 films et >= 20 ans de carrière
    valid_actors = actor_stats.filter(
        (F.col("nb_movies") >= 15) & 
        ((F.col("last_year") - F.col("first_year")) >= 20)
    )

    # 6. Calcul du pic précoce
    # On trie les films de chaque acteur par année
    w = Window.partitionBy("nconst").orderBy("startYear")
    actor_movies_ranked = actor_movies.withColumn("rank", F.row_number().over(w))
    
    # On joint avec le nombre total de films pour trouver la moitié
    actor_movies_ranked = actor_movies_ranked.join(valid_actors, "nconst")
    actor_movies_ranked = actor_movies_ranked.withColumn(
        "half", F.ceil(F.col("nb_movies") / 2)
    )

    # Moyenne de la première moitié de carrière
    first_half = actor_movies_ranked.filter(F.col("rank") <= F.col("half")) \
        .groupBy("nconst").agg(F.avg("averageRating").alias("avg_first_half"))

    # Moyenne de la deuxième moitié de carrière
    second_half = actor_movies_ranked.filter(F.col("rank") > F.col("half")) \
        .groupBy("nconst").agg(F.avg("averageRating").alias("avg_second_half"))

    # 7. Résultat final : première moitié > deuxième moitié
    result = valid_actors \
        .join(first_half, "nconst") \
        .join(second_half, "nconst") \
        .filter(F.col("avg_first_half") > F.col("avg_second_half")) \
        .join(names.select("nconst", "primaryName"), "nconst") \
        .select(
            "nconst", "primaryName", "first_year", "last_year", 
            "nb_movies", F.round("avg_first_half", 2).alias("avg_early"), 
            F.round("avg_second_half", 2).alias("avg_late")
        ).orderBy(F.desc("nb_movies"))

    print(f"Total actors with early peak: {result.count()}")
    result.show(20, truncate=False)

    # 8. Sauvegarde
    out_dir = OUTPUT_DIR.parent / "exploration" / "q7_early_peaks"
    result.coalesce(1).write.mode("overwrite").option("header", True).csv(str(out_dir))
    print(f"Written to {out_dir}")


if __name__ == "__main__":
    main()
