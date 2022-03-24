# Spark-Learning

### Spark config
```
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
```

