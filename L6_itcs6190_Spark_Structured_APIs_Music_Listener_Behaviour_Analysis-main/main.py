# main.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import os

def write_single_csv(df, path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.toPandas().to_csv(path, index=False)

spark = SparkSession.builder.appName("MusicAnalysis").getOrCreate()

# Load datasets
listening_logs = spark.read.option("header", True).option("inferSchema", True).csv("listening_logs.csv")
songs_metadata = spark.read.option("header", True).option("inferSchema", True).csv("songs_metadata.csv")
listening_logs = listening_logs.withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))
df = listening_logs.join(songs_metadata, on="song_id", how="inner")
out_base = "outputs"

# Task 1: User Favorite Genres
user_genre_stats = df.groupBy("user_id", "genre").agg(count(lit(1)).alias("listen_count"),sum(col("duration_sec")).alias("total_listen_seconds"))
user_top_genre_window = Window.partitionBy("user_id").orderBy(desc("listen_count"), desc("total_listen_seconds"),asc("genre"))
user_favorite_genre = (user_genre_stats.withColumn("rank", row_number().over(user_top_genre_window)).filter(col("rank") == 1).drop("rank").orderBy("user_id"))


# Task 2: Average Listen Time
user_avg_listen_time  = (df.groupBy("user_id").agg(round(avg(col("duration_sec")), 2).alias("avg_duration_sec")).orderBy(desc("avg_duration_sec")))


# Task 3: Create your own Genre Loyalty Scores and rank them and list out top 10
user_total = df.groupBy("user_id").agg(count(lit(1)).alias("total_plays_user"))
user_genre = df.groupBy("user_id", "genre").agg(count(lit(1)).alias("plays_in_genre"),sum(col("duration_sec")).alias("seconds_in_genre")).join(user_total, on="user_id", how="inner")
genre_loyalty_top10 = (user_genre.withColumn("share_of_user", col("plays_in_genre") / col("total_plays_user")).withColumn("loyalty_score", round(col("share_of_user") * log1p(col("seconds_in_genre")), 6)).orderBy(desc("loyalty_score"), desc("plays_in_genre"), desc("seconds_in_genre")).limit(10))


# Task 4: Identify users who listen between 12 AM and 5 AM
late_night_users = (df.withColumn("hour", hour(col("timestamp"))).filter((col("hour") >= 0) & (col("hour") < 5)).select("user_id").distinct())

write_single_csv(user_favorite_genre, f"{out_base}/user_favorite_genres.csv")
write_single_csv(user_avg_listen_time, f"{out_base}/avg_listen_time.csv")
write_single_csv(genre_loyalty_top10, f"{out_base}/top10_genre_loyalty.csv")
write_single_csv(late_night_users, f"{out_base}/late_night_users.csv")
spark.stop()

