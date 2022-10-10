from pyspark.sql import SparkSession


spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("OFF")

playData = sc.textFile('shakespearePlays.txt', 2)

print("Count: ", playData.count())
print("Counting the Number of Characters on Each Line: ", playData.map(lambda x: len(x)).sum())