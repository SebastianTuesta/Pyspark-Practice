from pyspark.sql import SparkSession


spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("OFF")

pythonList = ['b' , 'd', 'm', 't', 'e', 'u']
RDD1 = sc.parallelize(pythonList,2)


def vowelCheckFunction(data):
    if data in ['a','e','i','o','u']:
        return 1
    else :
        return 0

RDD2 = RDD1.map(lambda data: (data, vowelCheckFunction(data)))

print("Keys: ", RDD2.keys().collect())
print("Values: ", RDD2.values().collect())