from struct import Struct
from tokenize import Double
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("OFF")

schema = StructType(
    [
        StructField("age", IntegerType(), True),
        StructField("workclass", StringType(), True),
        StructField("fnlwgt", DoubleType(), True),
        StructField("education", StringType(), True),
        StructField("education-num", DoubleType(), True),
        StructField("marital-status", StringType(), True),
        StructField("occupation", StringType(), True),
        StructField("relationship", StringType(), True),
        StructField("race", StringType(), True),
        StructField("sex", StringType(), True),
        StructField("capital-gain", DoubleType(), True),
        StructField("capital-loss", DoubleType(), True),
        StructField("hours-per-week", DoubleType(), True),
        StructField("native-country", StringType(), True),
        StructField("income", StringType(), True)
    ]
)

censusDataFrame = spark.read.csv('adult.data',header=False, schema=schema, mode="DROPMALFORMED")
censusDataFrame.createOrReplaceTempView("censusDataTable")

censusDataAgeIncome = spark.sql('select age, income from censusDataTable limit 5')
censusDataAgeIncome.show()

avgHoursPerWeekByEducation = spark.sql("select education, round(avg(`hours-per-week`),2) as averageHoursPerWeek from censusDataTable group by education")
avgHoursPerWeekByEducation.show()