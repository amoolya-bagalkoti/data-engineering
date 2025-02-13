from pyspark.sql import SparkSession
from pyspark.sql.functions import col, coalesce, array, when, lit, expr, row_number, explode, collect_list
from pyspark.sql.window import Window

# Initialize Spark session
spark = SparkSession.builder.appName("ActorsProcessing").getOrCreate()

# Read data from the actors and actor_films tables
actors_df = spark.read.option("header", "true").csv("actors.csv")
actor_films_df = spark.read.option("header", "true").csv("actor_films.csv")

# Filter data for last year and this year
last_year_df = actors_df.filter(col("current_year") == 1973)
this_year_df = actor_films_df.filter(col("year") == 1974)

# Process the data
processed_df = last_year_df.alias("ly").join(
    this_year_df.alias("ty"),
    (col("ly.actor_id") == col("ty.actorid")) & (col("ly.actor_name") == col("ty.actor")),
    "full_outer"
).select(
    coalesce(col("ly.actor_id"), col("ty.actorid")).alias("actor_id"),
    coalesce(col("ly.actor_name"), col("ty.actor")).alias("actor_name"),
    expr("coalesce(ly.films, array()) || CASE WHEN ty.film IS NOT NULL THEN array(struct(ty.year, ty.film, ty.votes, ty.rating, ty.filmid)) ELSE array() END").alias("films"),
    when(col("ty.film").isNotNull(),
         when(col("ty.rating") > 8, lit("star"))
         .when(col("ty.rating") > 7, lit("good"))
         .when(col("ty.rating") > 6, lit("average"))
         .otherwise(lit("bad"))
         ).otherwise(col("ly.quality_class")).alias("quality_class"),
    col("ty.year").isNotNull().alias("is_active"),
    when(col("ty.film").isNotNull(), lit(0)).otherwise(col("ly.years_since_last_active") + 1).alias("years_since_last_active"),
    coalesce(col("ty.year"), col("ly.current_year") + 1).alias("current_year")
)

# Explode the films array and prepare for aggregation
exploded_df = processed_df.withColumn("film", explode("films"))

# Aggregate the data
aggregated_df = exploded_df.groupBy("actor_id", "actor_name").agg(
    collect_list("film").alias("films"),
    expr("MAX(quality_class)").alias("quality_class"),
    expr("MAX(CASE WHEN is_active THEN 1 ELSE 0 END)").cast("boolean").alias("is_active"),
    expr("MAX(years_since_last_active)").alias("years_since_last_active"),
    expr("MAX(current_year)").alias("current_year")
)

# Write the results back to the actors table (assuming a CSV format for simplicity)
aggregated_df.write.mode("overwrite").csv("actors_updated.csv")

# Stop the Spark session
spark.stop()