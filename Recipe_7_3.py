def stringToNumberSum(data):
    removedSpaceData = data.strip()
    
    
    if removedSpaceData == '' :
        return(None)
    
    splittedData = removedSpaceData.split(' ')
    numData = [float(x) for x in splittedData]
    sumOfData = sum(numData)
    return (sumOfData)

from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession


spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("OFF")

pysparkBookStreamingContext = StreamingContext(sc, 10)

consoleStreamingData = pysparkBookStreamingContext.socketTextStream(
    hostname = 'localhost',
    port = 55342
)
sumedData = consoleStreamingData.map(stringToNumberSum)
print(sumedData.pprint())
pysparkBookStreamingContext.start(); pysparkBookStreamingContext.awaitTermination()

# ncat -l -p 55342