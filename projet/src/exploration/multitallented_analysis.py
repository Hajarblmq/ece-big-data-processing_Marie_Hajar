"""Q8: multi-talented film professionals (actor / director / writer) and the
performance of their movies when they hold several roles on the same movie."""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from ..spark_session import get_spark
from .common import load_clean, save_csv


def role_credits(principals: DataFrame, basics: DataFrame,
                 ratings: DataFrame, min_votes: int) -> DataFrame:
    """One row per (person, movie, role) for rated movies.

    Roles: actor (actor or actress), director, writer. Only movies with at
    least `min_votes` votes are kept.
    """
    movies = (
        basics.filter(F.col("titleType") == "movie").select("tconst")
        .join(ratings.filter(F.col("numVotes") >= min_votes)
              .select("tconst", "averageRating", "numVotes"), "tconst")
    )
    role = (F.when(F.col("category").isin("actor", "actress"), "actor")
             .when(F.col("category") == "director", "director")
             .when(F.col("category") == "writer", "writer"))
    credits = (principals.withColumn("role", role)
               .filter(F.col("role").isNotNull())
               .select("nconst", "tconst", "role"))
    # Join with the (small) movie table before the distinct to limit the shuffle.
    return (credits.join(F.broadcast(movies), "tconst")
            .select("nconst", "tconst", "role", "averageRating", "numVotes")
            .distinct())


def multi_talented(credits: DataFrame, names: DataFrame,
                   min_movies_per_role: int = 3) -> DataFrame:
    """People who hold at least 2 roles, each in >= `min_movies_per_role`
    movies, with the number of movies per role and their mean rating."""
    per_role = (credits.groupBy("nconst", "role")
                .agg(F.countDistinct("tconst").alias("n_movies"))
                .filter(F.col("n_movies") >= min_movies_per_role))
    people = per_role.groupBy("nconst").agg(
        F.count("*").alias("n_roles"),
        F.array_join(F.array_sort(F.collect_list("role")), "+").alias("roles"),
        F.sum(F.when(F.col("role") == "actor", F.col("n_movies"))
              .otherwise(0)).alias("movies_as_actor"),
        F.sum(F.when(F.col("role") == "director", F.col("n_movies"))
              .otherwise(0)).alias("movies_as_director"),
        F.sum(F.when(F.col("role") == "writer", F.col("n_movies"))
              .otherwise(0)).alias("movies_as_writer"),
    ).filter(F.col("n_roles") >= 2)
    mean_rating = (credits.select("nconst", "tconst", "averageRating")
                   .distinct().groupBy("nconst")
                   .agg(F.round(F.avg("averageRating"), 2).alias("avg_rating")))
    return (people.join(mean_rating, "nconst")
            .join(names.select("nconst", "primaryName"), "nconst")
            .select("nconst", "primaryName", "roles", "n_roles",
                    "movies_as_actor", "movies_as_director",
                    "movies_as_writer", "avg_rating")
            .orderBy(F.desc("n_roles"),
                     F.desc(F.col("movies_as_actor")
                            + F.col("movies_as_director")
                            + F.col("movies_as_writer"))))


def roles_per_movie(credits: DataFrame, talented: DataFrame) -> DataFrame:
    """For each multi-talented person and movie: the roles held on it."""
    return (credits.join(talented.select("nconst"), "nconst")
            .groupBy("nconst", "tconst", "averageRating", "numVotes")
            .agg(F.array_join(F.array_sort(F.collect_set("role")), "+")
                  .alias("combo"),
                 F.countDistinct("role").alias("n_roles_in_movie")))


def combination_summary(per_movie: DataFrame) -> DataFrame:
    """Mean rating and votes per combination of roles on the same movie."""
    return (per_movie.groupBy("combo", "n_roles_in_movie").agg(
        F.countDistinct("nconst").alias("n_people"),
        F.count("*").alias("n_person_movies"),
        F.round(F.avg("averageRating"), 2).alias("avg_rating"),
        F.round(F.avg("numVotes"), 0).alias("avg_votes"))
        .orderBy("n_roles_in_movie", "combo"))


def paired_comparison(per_movie: DataFrame, names: DataFrame,
                      min_each: int = 2) -> DataFrame:
    """Per person: mean rating of the movies where he or she holds several
    roles versus the movies where he or she holds only one."""
    multi = F.col("n_roles_in_movie") >= 2
    per_person = per_movie.groupBy("nconst").agg(
        F.sum(F.when(multi, 1).otherwise(0)).alias("n_multi"),
        F.sum(F.when(~multi, 1).otherwise(0)).alias("n_single"),
        F.avg(F.when(multi, F.col("averageRating"))).alias("avg_multi"),
        F.avg(F.when(~multi, F.col("averageRating"))).alias("avg_single"),
    )
    both = (per_person
            .filter((F.col("n_multi") >= min_each)
                    & (F.col("n_single") >= min_each))
            .withColumn("diff", F.col("avg_multi") - F.col("avg_single")))
    return (both.join(names.select("nconst", "primaryName"), "nconst")
            .select("nconst", "primaryName", "n_multi", "n_single",
                    F.round("avg_multi", 2).alias("avg_multi"),
                    F.round("avg_single", 2).alias("avg_single"),
                    F.round("diff", 2).alias("diff")))


def main(min_votes: int = 1000) -> None:
    spark = get_spark("Exploration-MultiTalented")
    spark.sparkContext.setLogLevel("ERROR")
    basics = load_clean(spark, "title_basics")
    ratings = load_clean(spark, "title_ratings")
    names = load_clean(spark, "name_basics")
    principals = load_clean(spark, "title_principals", drop_flagged=False)

    credits = role_credits(principals, basics, ratings, min_votes).cache()
    talented = multi_talented(credits, names).cache()
    print(f"Q8a - multi-talented people (min_votes={min_votes}): "
          f"{talented.count()}")
    talented.groupBy("roles").count().orderBy(F.desc("count")).show(10, False)
    talented.show(20, False)

    per_movie = roles_per_movie(credits, talented).cache()
    combos = combination_summary(per_movie)
    print("Q8b - rating by combination of roles on the same movie:")
    combos.show(20, False)

    paired = paired_comparison(per_movie, names).cache()
    print(f"Q8c - people with >= 2 multi-role and >= 2 single-role movies: "
          f"{paired.count()}")
    paired.agg(
        F.round(F.avg("avg_multi"), 3).alias("mean_rating_multi_role"),
        F.round(F.avg("avg_single"), 3).alias("mean_rating_single_role"),
        F.round(F.avg("diff"), 3).alias("mean_difference"),
        F.round(F.avg(F.when(F.col("diff") > 0, 1).otherwise(0)), 3)
         .alias("share_better_when_multi_role"),
    ).show(truncate=False)
    paired.orderBy(F.desc("n_multi")).show(20, False)

    save_csv(talented, "q8_multitalented_people")
    save_csv(combos, "q8_role_combinations")
    save_csv(paired, "q8_paired_comparison")


if __name__ == "__main__":
    main()
