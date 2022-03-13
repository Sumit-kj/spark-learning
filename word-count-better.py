import utils
import re


def normalizeWord(book):
    print(type(book))
    return re.compile(r'\W+', re.UNICODE).split(book.lower())


sc = utils.get_spark_context("local", "WordCount")
rdd_book = sc.textFile(utils.root_file_dir + "Book")
rdd_words = rdd_book.flatMap(normalizeWord)
rdd_word_count = rdd_words.countByValue()
utils.print_rdd_dict_clean_ascii(rdd_word_count)
