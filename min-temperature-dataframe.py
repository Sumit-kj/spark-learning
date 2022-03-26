import utils
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType

spark = utils.get_spark_session("MinTemperature")

schema = StructType([
    StructField("station_iD", StringType(), True),
    StructField("date", IntegerType(), True),
    StructField("measure_type", StringType(), True),
    StructField("temperature", FloatType(), True)
])

df = spark.read.schema(schema).csv(utils.root_file_dir + "1800.csv")
df.printSchema()

min_temp = df.filter(df.measure_type == "TMIN")

station_temps = min_temp.select("station_id", "temperature")

min_temp_by_stations = station_temps.groupBy("station_id").min("temperature")
min_temp_by_stations.show()

min_temp_by_station_in_f = min_temp_by_stations.withColumn("temperature",
                                                           func.round(func.col("min(temperature)") * 0.1 * (9.0 / 5.0)
                                                                      + 32.0, 2)).select("station_id", "temperature")\
    .sort("temperature")

min_temp_by_station_in_f.show()

spark.stop()

