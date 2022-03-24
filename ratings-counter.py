import collections
import utils

sc = utils.get_spark_context("local", "RatingsHistogram")

lines = sc.textFile(utils.root_file_dir + "ml-100k/u.data")
ratings = lines.map(lambda x: x.split()[2])
result = ratings.countByValue()

sortedResults = collections.OrderedDict(sorted(result.items()))
utils.print_rdd_dict(sortedResults)
