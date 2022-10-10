from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("OFF")

ls = [2.3, 3.4, 4.3, 2.4, 2.3, 4.0]

rdd = sc.parallelize(ls)
print("Action: First element: " + str(rdd.first()))
print("Action: 2 first element: " + str(rdd.take(2)))
print("Number of Partitions: " + str(rdd.getNumPartitions()))
