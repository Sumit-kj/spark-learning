import utils


def parseLine(line):
    fields = line.split(',')
    customer_id = int(fields[0])
    amount = float(fields[2])
    return customer_id, amount


sc = utils.get_spark_context("local", "TotalSpentByCustomer")
lines = sc.textFile(utils.root_file_dir + "customer-orders.csv")
rdd_lines = lines.map(parseLine)
rdd_total_amount = rdd_lines.reduceByKey(lambda x, y: x+y)
rdd_total_amount_sorted = rdd_total_amount.map(lambda x: (x[1], x[0])).sortByKey()
utils.print_rdd(rdd_total_amount_sorted)
