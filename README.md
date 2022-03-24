# Spark-Learning

## Spark RDD's

### Spark config
```
from pyspark import SparkConf, SparkContext
conf = SparkConf().setMaster("local").setAppName("app_name")
spark_context = SparkContext(conf=conf)
```

### Reading from text file
```
lines = sc.textFile("path_to_text_file")
```

### Fetching data from lines
```
data_to_be_processed = lines.map(lambda_or_function)

lambda: 
lambda x: x.split()[1]

function: 
def parse_line(line):
    fields = line.,split(',')
    field_1 = fields[0]
    field_2 = fields[2]
    return (field_1, field_2) 
```

### Transforming RDD's
```
map:
Takes data in RDD to perform function to create another data set.
Same size as original RDD.

flatmap:
Can create mutiple values for each data in RDD.
Can be smaller or larger in size.

filters:
Filters the RDD

distinct:
Unique

sample:
Smaller snap of the RDD.
Useful when testing on large RDD

union, intersection,subtract, cartesian product:
Set operations on 2 different RDD's
```

```
If keys aren't getting modified use mapValues(), flatMapvalues() instead of map(), flatMap().
It allows spark to amintain the original RDD instead of haveing to shuffle the data around, which can be expensive on cluster.
```

### Actions on RDD
```
collect: 
Dumps out all data, prints it.

count:
Returns the count of all values.

countByValue:
Returns count of unique values.

take, top:
Return a sample from the RDD.

reduce:
Combines all different values for a given key-value and gives aggregation on them.

reduceByKey:
Combines values with the same key using some function.

groupByKey:
Group values with same keys

sortByKey:
Sorts the RDD based on the key

keys:
Returns all the keys for the RDD

values:
Returns all the values for the RDD 
```

## Spark-SQL

### SparkSession

```
from pyspark.sql import SparkSession
spark_session = SparkSession.builder.appName("app_name").getOrCreate()
```

### Reading from file
```
lines = spark_session.SparkContext.textFile("file_path")
data_to_be_processed = lines.map(mapper_function)
```

### Row based map functions
```
from pyspark.sql import Row
def mapper_function(line):
    fields = line.split(',')
    return Row(ID=fields[0], name=fields[1],...)
```

### Inferred Schema
```
inferred_schema = spark.read.option("header", "true").option("inferSchema", "true").csv(file_path)
inferred_schema.printSchema()
```

### SparkSQL dataframe and TempView
```
schema_data = spark_session.createDataFrame("df_name").cache()
scheam_data.createOrReplaceTempView("df_name")
```

### SparkSQL Query
```
result = spark_session.sql("SQL_QUERY")
```

### SparkSession functions
```
select:
Selects the specified columns as parameter and returns df of the same
result_select = inferred_schema.select("column_name")

show:
Shows the df invoking the method
result_select.show()

filter:
Filters the df with the condition provided
result_select.filter(inferred_schema.coulumn_name > 10).show()

groupby:
Groups the df based upon some column
result_select.groupby("column_name").show()

sort:
Sorts the df based on the column passed
result_select.sort("column_name").show()

agg:
Provides an aggregate function on the non group by column
from pyspark import functions as func
result_select.groupBy("column_name").agg(func.round(func.avg("coulumn_name_2"),2)).show()

alias:
Provides an alias to the SQL select column
result_select.groupBy("column_name").agg(func.round(func.avg("coulumn_name_2"),2).alias("avg")).show()
```
```
column_names in double quotes is as good as inferred_schema.columns_name
```

### Closing SparkSession
```
spark_session_close()
```