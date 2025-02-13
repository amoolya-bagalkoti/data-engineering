from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, lag, sum, min, max, when, expr

# Initialize Spark session
spark = SparkSession.builder.appName("BackfillActorsHistorySCD").getOrCreate()

# Load data from the actors table
actors_df = spark.read.option("header", "true").csv("/path/to/actors.csv")

# Convert columns to appropriate data types
actors_df = actors_df.withColumn("current_year", col("current_year").cast("int")) \
                     .withColumn("is_active", col("is_active").cast("boolean"))

# Detect changes
window_spec = Window.partitionBy("actor_name").orderBy("current_year")

changes_detected_df = actors_df.filter(col("current_year") <= 1972) \
    .withColumn("prev_quality_class", lag("quality_class").over(window_spec)) \
    .withColumn("prev_is_active", lag("is_active").over(window_spec)) \
    .withColumn("has_changed", (col("quality_class") != col("prev_quality_class")) | 
                                (col("is_active") != col("prev_is_active")) |
                                col("prev_quality_class").isNull() |
                                col("prev_is_active").isNull())

# Identify streaks
streak_identified_df = changes_detected_df \
    .withColumn("streak_identifier", sum(when(col("has_changed"), 1).otherwise(0)).over(window_spec))

# Aggregate data
aggregated_df = streak_identified_df.groupBy("actor_id", "actor_name", "quality_class", "is_active", "streak_identifier") \
    .agg(min("current_year").alias("start_year"),
         max("current_year").alias("end_year")) \
    .withColumn("current_year", expr("1972"))

# Write the results into the actors_history_scd table (assuming a CSV format for simplicity)
aggregated_df.write.mode("overwrite").csv("/path/to/actors_history_scd.csv")

# Stop the Spark session
spark.stop()