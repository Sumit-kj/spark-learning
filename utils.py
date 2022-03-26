from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

root_file_dir = "file:///Users/sumit/PycharmProjects/spark-learning/"
root_dir = "C:/Users/sumit/PycharmProjects/spark-learning/"


def print_rdd(rdd):
    dataset = rdd.collect()
    for data in dataset:
        print(data)


def print_rdd_count(rdd):
    dataset = rdd.collect()
    for data in dataset:
        count = str(data[0])
        clean_key = data[1].encode('ascii', 'ignore')
        if clean_key:
            print(clean_key.decode() + ":\t\t\t" + count)


def print_rdd_dict(rdd):
    for key, value in rdd.items():
        print(key, value)


def print_rdd_dict_clean_ascii(rdd):
    for key, value in rdd.items():
        clean_key = key.encode('ascii', 'ignore')
        if clean_key and '\ufffd' not in key:
            print(key, value)


def get_spark_context(master="local", app_name="App"):
    conf = SparkConf().setMaster(master).setAppName(app_name)
    return SparkContext(conf=conf)


def get_spark_session(app_name="App"):
    return SparkSession.builder.appName(app_name).getOrCreate()
