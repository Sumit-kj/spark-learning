import codecs
import utils
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, LongType


def load_movie_mapping():
    movie_map = {}
    with codecs.open(utils.root_dir + 'ml-100k/u.item', "r", encoding='ISO-8859-1', errors='ignore') as f:
        for line in f:
            fields = line.split('|')
            movie_map[int(fields[0])] = fields[1]
    return movie_map


spark = utils.get_spark_session("PopularMovieNames")
name_map = spark.sparkContext.broadcast(load_movie_mapping())

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


def lookup_name(movie_id):
    return name_map.value[movie_id]


lookup_name_udf = func.udf(lookup_name)

movies_popularity_name = movie_popularity.withColumn("movie_name", lookup_name_udf(func.col("movie_id")))

movie_popularity_sorted = movies_popularity_name.orderBy(func.desc("review_count"))
movie_popularity_sorted.show(movie_popularity_sorted.count())

spark.stop()
