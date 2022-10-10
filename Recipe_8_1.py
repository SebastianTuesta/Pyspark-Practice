from pyspark.sql import SparkSession, Row, SQLContext
from pyspark.sql.types import *

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("OFF")

sqlContext = SQLContext(sc)


filamentData = [['filamentA','100W',605],
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
    ['filamentA','200W',541]
]

FilamentTypeColumn = StructField("FilamentType",StringType(),True)
BulbPowerColumn = StructField("BulbPower",StringType(),True)
LifeInHoursColumn = StructField("LifeInHours",IntegerType(),True)


FilamentDataFrameSchema = StructType([FilamentTypeColumn, BulbPowerColumn, LifeInHoursColumn])
filamentDataRDD = sc.parallelize(filamentData, 4)
filamentDataFrameRaw = sqlContext.createDataFrame(filamentDataRDD, FilamentDataFrameSchema)

print(filamentDataFrameRaw.show(4))
