import time
import sys
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import count, desc, sequence, sum

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

# EX 1 A
################################################################################

# read csv files as data frame
df_rec = spark.read.csv(r'C:\Users\ofir\Downloads\drive-download-20230531T104531Z-001\recommendations.csv',
                        header=True, inferSchema=True)

df_games = spark.read.csv(r'C:\Users\ofir\Downloads\drive-download-20230531T104531Z-001\games.csv',
                          header=True, inferSchema=True)

# order all recommendations by funny with condition of hours > 20
top_funniest_rec_df = df_rec.filter(df_rec['hours'] > 20).select('app_id', 'user_id', 'funny', 'hours').orderBy(
    desc('funny'))

# get the top funniest recommendations
top_10_rec_df = top_funniest_rec_df.limit(10)

# join with games dataset and alias every "column"
top_10_rec_with_game_name_df = top_10_rec_df.join(df_games, ["app_id"], "inner").select(
    df_games["title"].alias("game_name"), top_10_rec_df["user_id"].alias("user_id"),
    top_10_rec_df["funny"].alias("num_funny"), top_10_rec_df["hours"].alias("hours_played")).orderBy(desc("num_funny"))

top_10_rec_with_game_name_df.show()

# serialized to parquet file
top_10_rec_with_game_name_df.write.mode('overwrite').parquet(
    r'C:\Users\ofir\Downloads\drive-download-20230531T104531Z-001\outputs\funniest_recommendation')

################################################################################

# EX 1 B
################################################################################

df_users = spark.read.csv(r'C:\Users\ofir\Downloads\drive-download-20230531T104531Z-001\users.csv',
                          header=True, inferSchema=True)

# group by user_id and sum all reviews accordingly
top_reviews_df = df_users.groupBy('user_id').agg(sum('reviews').alias("reviews_num"))

# get the top 50 users with most reviews
order_reviews = top_reviews_df.orderBy(desc('reviews_num')).limit(50)

order_reviews.show(10)

# join with recommendation dataframe to get the helpful number per user.
# after that group by again and sum all helpful in addition
# then, order again by reviews_num column
top_50_reviews_with_helpful_df = order_reviews.join(df_rec, ["user_id"], "inner").select(
    df_rec["user_id"].alias("user_id"),
    order_reviews["reviews_num"].alias("reviews_num"),
    df_rec["helpful"].alias("helpful")) \
    .groupBy("user_id", "reviews_num").agg(sum("helpful").alias("helpful_num")) \
    .orderBy(desc("reviews_num"))

top_50_reviews_with_helpful_df.show(10)

# serialized to parquet file
top_50_reviews_with_helpful_df.write.mode('overwrite').parquet(
    r'C:\Users\ofir\Downloads\drive-download-20230531T104531Z-001\outputs\top_reviews')

input()
