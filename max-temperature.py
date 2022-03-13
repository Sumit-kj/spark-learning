import utils


def parseLine(line):
    fields = line.split(',')
    station_id = fields[0]
    entry_type = fields[2]
    temperature = float(fields[3]) * 0.1 * (9.0/5.0) + 32
    return station_id, entry_type, temperature


sc = utils.get_spark_context("local", "MaximumTemperature")
lines = sc.textFile(utils.root_file_dir + "1800.csv")
rdd_lines = lines.map(parseLine)
rdd_max_temp = rdd_lines.filter(lambda x: "TMAX" in x[1])
rdd_max_temp_station = rdd_max_temp.map(lambda x: (x[0], x[2]))
rdd_max_temp_station_1800 = rdd_max_temp_station.reduceByKey(lambda x, y: round(max(x, y), 2))
utils.print_rdd(rdd_max_temp_station_1800)