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

# Orders count
df_orders = spark.read.parquet("./Data/sales_parquet/")
print("== Orders count ==", df_orders.count())

# Products count
df_products = spark.read.parquet("./Data/products_parquet/")
print("== Products count ==", df_products.count())

# Sellers count
df_sellers = spark.read.parquet("./Data/sellers_parquet/")
print("== Seller count ==", df_sellers.count())

# How many products have been sold at least once?
df_orders.select(countDistinct("product_id")).show()

# Which is the product contained in more orders?
df_orders.groupBy("product_id").agg(count('*').alias("num_orders")).orderBy(col("num_orders").desc()).show(1)