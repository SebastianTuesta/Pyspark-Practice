from pyspark.sql import SparkSession


spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("OFF")

import csv
from io import StringIO

def createCSV(dataList):
    data = StringIO()
    dataWriter = csv.writer(data,lineterminator='')
    dataWriter.writerow(dataList)
    return (data.getvalue())

simpleData = [
    ['p',20],
    ['q',30],
    ['r',20],
    ['m',25]
]

simpleRDD = sc.parallelize(simpleData,4)
simpleRDDLines = simpleRDD.map(createCSV)
simpleRDDLines.saveAsTextFile('/Users/tuest/OneDrive/Escritorio/Pyspark Practice/csvFiles/')