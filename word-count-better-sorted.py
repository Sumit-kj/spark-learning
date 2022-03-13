import utils
import re


def normalizeWord(book):
    return re.compile(r'\W+', re.UNICODE).split(book.lower())


sc = utils.get_spark_context("local", "WordCount")
rdd_book = sc.textFile(utils.root_file_dir + "Book")
rdd_words = rdd_book.flatMap(normalizeWord)
rdd_word_count = rdd_words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x+y)
rdd_word_count_sorted = rdd_word_count.map(lambda x: (x[1], x[0])).sortByKey()
utils.print_rdd_count(rdd_word_count_sorted)
