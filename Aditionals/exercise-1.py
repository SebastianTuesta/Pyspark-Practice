from pyspark import SparkSession
from pyspark.sql import functions as F

# Creating the Spark Session
spark = SparkSession.builder \
    .master("local") \
    .config("spark.sql.autoBroadcastJoinThreshold", -1) \
    .config("spark.executor.memory", "500mb") \
    .appName("Exercise1") \
    .getOrCreate()

df_orders = spark.read.parquet("/data/orders.parquet")
df_products = spark.read.parquet("/data/products.parquet")

df_orders.\
    join(df_products, df_orders.product_id == df_products.product_id, "inner").\
    groupBy().\
    avg(F.col('num_pieces_sold')*F.col('num_pieces_sold'))