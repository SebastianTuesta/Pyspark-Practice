from pyspark.sql import SparkSession


spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("OFF")

import csv
from io import StringIO

def parseCSV(csvRow) :
    data = StringIO(csvRow)
    dataReader = csv.reader(data)
    return(dataReader.next())

filamentRDD =sc.textFile('/filamentData.csv',4)
filamentRDDCSV = filamentRDD.map(parseCSV)