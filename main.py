import time
import sys
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import count, desc, sequence

# simple check only for test

# spark = SparkSession.builder.appName("testDataFrame").getOrCreate()
#
# data = [("java", "20000"), ("python", "100000"), ("Scala", "300")]
#
# df = spark.createDataFrame(data)
#
# df.show()
# df.write.mode('overwrite').parquet(
#     r'C:\Users\ofir\Downloads\drive-download-20230531T104531Z-001\outputs\example.parquet')

spark = SparkSession.builder.appName("FunniestMovie") \
    .config("spark.sql.crossJoin.enabled", "true").getOrCreate()

df_rec = spark.read.csv(r'C:\Users\ofir\Downloads\drive-download-20230531T104531Z-001\recommendations.csv',
                        header=True, inferSchema=True)

df_games = spark.read.csv(r'C:\Users\ofir\Downloads\drive-download-20230531T104531Z-001\games.csv',
                          header=True, inferSchema=True)

top_funniest_rec_df = df_rec.filter(df_rec['hours'] > 20).select('app_id', 'user_id', 'funny', 'hours').orderBy(
    desc('funny'))

top_10_rec_df = top_funniest_rec_df.limit(10)

top_10_rec_with_game_name_df = top_10_rec_df.join(df_games, ["app_id"], "inner").select(
    df_games["title"].alias("game_name"), top_10_rec_df["user_id"].alias("user_id"),
    top_10_rec_df["funny"].alias("num_funny"), top_10_rec_df["hours"].alias("hours_played"))

top_10_rec_with_game_name_df.show()

top_10_rec_with_game_name_df.write.mode('overwrite').parquet(
    r'C:\Users\ofir\Downloads\drive-download-20230531T104531Z-001\outputs\funniest_recommendation.parquet')

input()
