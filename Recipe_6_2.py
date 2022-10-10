from pyspark.sql import SparkSession


spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("OFF")

playData = sc.textFile('shakespearePlays.txt', 2)

playDataLineLength = playData.map(lambda x : len(x))
playDataLineLength.saveAsTextFile('/Users/tuest/OneDrive/Escritorio/Pyspark Practice/savedData')