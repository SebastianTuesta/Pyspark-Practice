from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("OFF")

data2001 = ['RIN1', 'RIN2', 'RIN3', 'RIN4', 'RIN5', 'RIN6', 'RIN7']
data2002 = ['RIN3', 'RIN4', 'RIN7', 'RIN8', 'RIN9']
data2003 = ['RIN4', 'RIN8', 'RIN10', 'RIN11', 'RIN12']

parData2001 = sc.parallelize(data2001,2)
parData2002 = sc.parallelize(data2002,2)
parData2003 = sc.parallelize(data2003, 2)

unionOf20012002 = parData2001.union(parData2002)

allResearchs = unionOf20012002.union(parData2003)

allUniqueResearchs = allResearchs.distinct()

print("== Distinct" , allResearchs.distinct().count())

unionTwoYears = parData2001.union(parData2002)
unionTwoYears.subtract(parData2003).collect()

print("== Finding Projects Completed in the First Two Years", unionTwoYears.collect())

projectsInTwoYear = parData2001.intersection(parData2002)

print("== Finding Projects Started in 2001 and Continued Through 2003.", projectsInTwoYear.collect())