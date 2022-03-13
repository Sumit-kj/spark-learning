import utils
from pyspark.sql import functions as func

spark = utils.get_spark_session("FriendsAgeDataFrame")

df_friends = spark.read.option("header", "true").option("inferSchema", "true") \
    .csv(utils.root_file_dir + "fakefriends-header.csv")

df_friends_and_age = df_friends.select("age", "friends")

df_friends_and_age.groupBy("age").avg("friends").sort("age").show()

df_friends_and_age.groupBy("age").agg(func.round(func.avg("friends"), 2)).sort("age").show()

df_friends_and_age.groupBy("age").agg(func.round(func.avg("friends"), 2).alias("friends_avg")).sort("age").show()

spark.stop()
