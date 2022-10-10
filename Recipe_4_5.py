from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("OFF")

airVelocityKMPH = [12,13,15,12,11,12,11]
parVelocityKMPH = sc.parallelize(airVelocityKMPH, 2)

countValue = parVelocityKMPH.count()
sumValue = parVelocityKMPH.sum()
meanValue = parVelocityKMPH.mean()
sampleVarianceValue = parVelocityKMPH.sampleVariance()

stdevValue = parVelocityKMPH.stdev()
sampleStdevValue = parVelocityKMPH.sampleStdev()

print("sampleStdevValue: ", sampleStdevValue)

print("Stats: ", parVelocityKMPH.stats().asDict())