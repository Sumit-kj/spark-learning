import utils
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType
from pyspark.sql import functions as func

spark = utils.get_spark_session("TotalSpentByCustomerDF")

schema = StructType([
    StructField('customer_id', IntegerType(), True),
    StructField('item_id', IntegerType(), True),
    StructField('amount', FloatType(), True)
])

input_df = spark.read.schema(schema).csv(utils.root_file_dir + 'customer-orders.csv')
input_df.printSchema()

spent_by_customer = input_df.select("customer_id", "amount")

total_spent_by_customer = spent_by_customer.groupBy("customer_id").sum("amount")

total_spent_by_customer_rounded = total_spent_by_customer.withColumn("total_spent",
                                                                     func.round(func.col("sum(amount)"), 2))

total_spent_by_customer_rounded_result = total_spent_by_customer_rounded.select("customer_id", "total_spent")
total_spent_by_customer_rounded_result_sorted = total_spent_by_customer_rounded_result.sort("total_spent")

total_spent_by_customer_rounded_result_sorted.show(total_spent_by_customer_rounded_result_sorted.count())

# From the course

total_by_customer = input_df.groupBy("customer_id").agg(func.round(func.sum("amount"), 2).alias("total_spent"))
total_by_customer_sorted = total_by_customer.sort("total_spent")

total_by_customer_sorted.show(total_by_customer_sorted.count())

spark.stop()


