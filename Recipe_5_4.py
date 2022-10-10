from pyspark.sql import SparkSession


spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("OFF")

pageLinks = [
    ['a' ,['b','c','d']],
    ['c', ['b']],
    ['b', ['d','c']],
    ['d', ['a','c']]
]

pageRanks = [
    ['a',1],
    ['c',1],
    ['b',1],
    ['d',1]
]

def rankContribution(uris, rank):
    numberOfUris = len(uris)
    rankContribution = float(rank) / numberOfUris
    newrank =[]
    for uri in uris:
            newrank.append((uri, rankContribution))
    return newrank

pageLinksRDD = sc.parallelize(pageLinks, 2)
pageRanksRDD = sc.parallelize(pageRanks, 2)


numIter = 20
s = 0.85

for i in range(numIter):
    linksRank = pageLinksRDD.join(pageRanksRDD)
    contributedRDD = linksRank.flatMap(lambda x : rankContribution(x[1]
[0],x[1][1]))
    sumRanks = contributedRDD.reduceByKey(lambda v1,v2 : v1+v2)
    pageRanksRDD = sumRanks.map(lambda x : (x[0],(1-s)+s*x[1]))

print("===")
print(pageRanksRDD.collect())
print("===")