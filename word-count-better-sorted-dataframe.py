from pyspark.sql import functions as func
import utils

spark = utils.get_spark_session("WordCount")

input_df = spark.read.text(utils.root_file_dir + "Book")

words = input_df.select(func.explode(func.split(input_df.value, "\\W+")).alias("word"))
words.filter(words.word != "")

lowercase_words = words.select(func.lower(words.word).alias("word"))

words_count = lowercase_words.groupBy("word").count()

words_count_sorted = words_count.sort("count")

words_count_sorted.show(words_count_sorted.count())
