import hashlib
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import hashlib

# Create the Spark session
spark = SparkSession.builder \
    .master("local") \
    .config("spark.sql.autoBroadcastJoinThreshold", -1) \
    .config("spark.executor.memory", "3g") \
    .appName("Exercise1") \
    .getOrCreate()

# Read the source tables
sales_table = spark.read.parquet("./data/sales_parquet")

# udf
def algo(order_id, bill_text):
    # if number is even
    ret = bill_text.encode('utf-8')
    if int(order_id) % 2 == 0:
        # Count number of 'A'
        cnt_A = bill_text.count('A')
        for _c in range(0, cnt_A):
            ret = hashlib.md5(ret).hexdigest().encode("utf-8")
        ret = ret.decode('utf-8')
    else:
        ret = hashlib.sha256(ret).hexdigest()
    return ret

algo_udf = spark.udf.register("algo", algo)

sales_table.withColumn("hashed_bill"
    , algo_udf(col("order_id"), col("bill_raw_text")))\
        .groupBy(col('hashed_bill')).agg(count("*").alias('cnt')).where(col('cnt')>1).show()