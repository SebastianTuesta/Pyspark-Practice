from pyspark.sql import SparkSession


spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("OFF")

filDataSingle = [['filamentA','100W',605],
    ['filamentB','100W',683],
    ['filamentB','100W',691],
    ['filamentB','200W',561],
    ['filamentA','200W',530],
    ['filamentA','100W',619],
    ['filamentB','100W',686],
    ['filamentB','200W',600],
    ['filamentB','100W',696],
    ['filamentA','200W',579],
    ['filamentA','200W',520],
    ['filamentA','100W',622],
    ['filamentA','100W',668],
    ['filamentB','200W',569],
    ['filamentB','200W',555],
    ['filamentA','200W',541]]

filDataSingleRDD = sc.parallelize(filDataSingle, 2)

filDataPairedRDD1 = filDataSingleRDD.map(lambda x : (x[0], [x[2], 1]))

powerSumandCount = filDataPairedRDD1.reduceByKey(
    lambda l1,l2 : [l1[0]+l2[0], l1[1]+l2[1]]
)

print(powerSumandCount.collect())