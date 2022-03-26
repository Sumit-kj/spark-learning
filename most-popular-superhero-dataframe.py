import utils
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = utils.get_spark_session("PopularSuperhero")

schema = StructType([
    StructField("hero_id", IntegerType(), True),
    StructField("hero_name", StringType(), True)
])

name_map_df = spark.read.option("sep", " ").schema(schema).csv(utils.root_file_dir + 'mcu/marvel-names.txt')
name_map_df.printSchema()

superhero_popularity_df = spark.read.text(utils.root_file_dir + 'mcu/marvel-graph.txt')

superhero_popularity_hero = superhero_popularity_df.withColumn("hero_id", func.split(func.col("value"), " ")[0])
superhero_popularity_hero_count = superhero_popularity_hero.withColumn("popularity",
                                                                       func.size(func.split(func.col("value"), " "))
                                                                       - 1).select("hero_id", "popularity")
superhero_popularity_hero_count_distinct = superhero_popularity_hero_count.groupBy("hero_id")\
    .agg(func.sum(func.col("popularity")).alias("connections"))

superhero_popularity_hero_count_distinct_sorted = superhero_popularity_hero_count_distinct.orderBy(
    func.desc("connections"))

most_popular_hero_id = superhero_popularity_hero_count_distinct_sorted.select("hero_id").first().hero_id

print(most_popular_hero_id)

most_popular_hero_name = name_map_df.filter(func.col("hero_id") == most_popular_hero_id).select("hero_name")
most_popular_hero_name.show()