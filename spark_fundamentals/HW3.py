from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, col, countDistinct, avg, count
from pyspark.sql import functions as F

# Initialize Spark session
spark = SparkSession.builder.appName("GameAnalytics").getOrCreate()

# Disable automatic broadcast join
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

# Load data
events = spark.read.option("header", "true").csv("/home/iceberg/data/events.csv")
matches = spark.read.option("header", "true").csv("/home/iceberg/data/matches.csv")
match_details = spark.read.option("header", "true").csv("/home/iceberg/data/match_details.csv")
medals_matches_players = spark.read.option("header", "true").csv("/home/iceberg/data/medals_matches_players.csv")
medals = spark.read.option("header", "true").csv("/home/iceberg/data/medals.csv")

# Convert event_time to date
events = events.withColumn('event_date', expr("DATE_TRUNC('day', event_time)"))
matches = matches.withColumn('partitioned_date', expr("DATE_TRUNC('day', completion_date)"))


# Broadcast `medals` and `maps`
broadcast_medals = F.broadcast(medals)
broadcast_matches = F.broadcast(matches)

# Bucket join on `match_id` with 16 buckets
bucketed_match_details = match_details.repartitionByRange(16, "match_id")
bucketed_matches = matches.repartitionByRange(16, "match_id")
bucketed_medals_matches_players = medals_matches_players.repartitionByRange(16, "match_id")

# Join the dataframes
joined_df = bucketed_match_details\
    .join(bucketed_matches, "match_id")\
    .join(bucketed_medals_matches_players, "match_id")\
    .join(broadcast_medals, "medal_id", "left")


# Which player averages the most kills per game?
player_avg_kills = joined_df.groupBy("player_gamertag")\
    .agg(avg("player_total_kills").alias("avg_kills"))\
    .orderBy(col("avg_kills").desc())
player_avg_kills.show()

# Which playlist gets played the most?
most_played_playlist = joined_df.groupBy("playlist_id")\
    .agg(count("match_id").alias("num_matches"))\
    .orderBy(col("num_matches").desc())
most_played_playlist.show()

# Which map gets played the most?
most_played_map = joined_df.groupBy("mapid")\
    .agg(count("match_id").alias("num_matches"))\
    .orderBy(col("num_matches").desc())
most_played_map.show()

# Which map do players get the most Killing Spree medals on?
killing_spree_medals = joined_df.filter(col("name") == "Killing Spree")\
    .groupBy("mapid")\
    .agg(count("medal_id").alias("num_killing_spree_medals"))\
    .orderBy(col("num_killing_spree_medals").desc())
killing_spree_medals.show()


# Try different .sortWithinPartitions to see which has the smallest data size
sorted_by_playlist = joined_df.sortWithinPartitions("playlist_id")
sorted_by_map = joined_df.sortWithinPartitions("mapid")

# Collect the size of the sorted dataframes
sorted_by_playlist_size = sorted_by_playlist.rdd.map(lambda row: len(row)).sum()
sorted_by_map_size = sorted_by_map.rdd.map(lambda row: len(row)).sum()

print(f"Data size sorted by playlist: {sorted_by_playlist_size}")
print(f"Data size sorted by map: {sorted_by_map_size}")


