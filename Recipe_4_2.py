from pyspark.sql import SparkSession

def fahrenheitToCentigrade(temperature):
    centigrade = (temperature-32)*5/9
    return centigrade

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("OFF")

tempData = [2.3, 3.4, 4.3, 2.4, 2.3, 4.0]

rdd1 = sc.parallelize(tempData)
rdd2 = rdd1.map(fahrenheitToCentigrade).filter(lambda temp: temp>=-16)

print("RDD: ", rdd2.collect())