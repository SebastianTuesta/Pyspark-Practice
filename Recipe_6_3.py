from pyspark.sql import SparkSession


spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("OFF")

playData = sc.wholeTextFiles('/Users/tuest/OneDrive/Escritorio/Pyspark Practice/manyFiles', 2)

print(playData.collect())