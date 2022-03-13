import utils


def parseLine(line):
    fields = line.split(',')
    age = int(fields[2])
    num_friends = int(fields[3])
    return age, num_friends


sc = utils.get_spark_context("local", "AverageFriendsByAge")
lines = sc.textFile(utils.root_file_dir + "fakefriends.csv")
rdd_lines = lines.map(parseLine)
rdd_age_occurrence = rdd_lines.mapValues(lambda x: (x, 1))
rdd_group_by_age = rdd_age_occurrence.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
rdd_average_friends = rdd_group_by_age.mapValues(lambda x: x[0] / x[1])
utils.print_rdd(rdd_average_friends)
