from pyspark.sql import SparkSession
from pyspark.sql.functions import countDistinct, col, count

# Creating the Spark Session
spark = SparkSession.builder \
    .master("local") \
    .config("spark.sql.autoBroadcastJoinThreshold", -1) \
    .config("spark.executor.memory", "500mb") \
    .appName("Exercise1") \
    .getOrCreate()

spark.sparkContext.setLogLevel('WARN')

df_orders = spark.read.parquet("./Data/sales_parquet/")
df_orders.groupBy('date').agg(countDistinct('product_id').alias("num_product")).orderBy(col("date").desc()).show()