from tokenize import Double
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("OFF")

sqlContext = SQLContext(sc)

FilementDataFrameSchema = StructType(
    [
        StructField("FilamentType", StringType(), True),
        StructField("BulbPower", StringType(), True),
        StructField("LifeInHours", DoubleType(), True)
    ]
)

filamentDataFrame = spark.read.csv('filamentData.csv', header=True, schema=FilementDataFrameSchema, mode="DROPMALFORMED")

print("== Top 5 ==")
print(filamentDataFrame.show(5))

print("== Schema ==")
filamentDataFrame.printSchema()

dataSummary = filamentDataFrame.describe()
print("== Summary ==")
dataSummary.show()

print("== Count == ")
print(filamentDataFrame.filter(filamentDataFrame.FilamentType == 'filamentA').count())