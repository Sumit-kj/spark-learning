import utils
from pyspark.sql.types import StructType, StructField, IntegerType, LongType
from pyspark.sql import functions as func

spark = utils.get_spark_session("PopularMovies")

schema = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("movie_id", IntegerType(), True),
    StructField("rating", IntegerType(), True),
    StructField("timestamp", LongType(), True)
])

input_df = spark.read.option("sep", "\t").schema(schema).csv(utils.root_file_dir + 'ml-100k/u.data')
input_df.printSchema()

movie_popularity = input_df.select("movie_id", "rating").groupBy("movie_id").agg(func.count("rating")
                                                                                 .alias("review_count"))

movie_popularity_sorted = movie_popularity.orderBy(func.desc("review_count"))
movie_popularity_sorted.show(movie_popularity_sorted.count())

spark.stop()
