from pyspark.sql import SparkSession


spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("OFF")

studentData = [['si1','Robin','M'],
    ['si2','Maria','F'],
    ['si3','Julie','F'],
    ['si4','Bob', 'M'],
    ['si6','William','M']]

subjectsData = [['si1','Python'],
    ['si3','Java'],
    ['si1','Java'],
    ['si2','Python'],
    ['si3','Ruby'],
    ['si4','C++'],
    ['si5','C'],
    ['si4','Python'],
    ['si2','Java']]

studentRDD = sc.parallelize(studentData, 2)
studentPairedRDD = studentRDD.map(lambda val : (val[0],[val[1],val[2]]))

subjectsPairedRDD = sc.parallelize(subjectsData, 2)


studenSubjectsInnerJoin = studentPairedRDD.join(subjectsPairedRDD)
print("studenSubjectsInnerJoin: ", studenSubjectsInnerJoin.collect())