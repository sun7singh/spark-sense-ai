from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast, col

spark = SparkSession.builder.appName("customer-orders-join").getOrCreate()

orders_df = spark.read.parquet("s3://my-bucket/raw/orders/")
customers_df = spark.read.parquet("s3://my-bucket/raw/customers/")

# This broadcast join assumes customers_df is small - but it has grown
# to several GB in production, causing the driver/executors to run out
# of memory trying to build the broadcast hash table.
joined_df = orders_df.join(broadcast(customers_df), on="customer_id", how="left")

result_df = joined_df.groupBy("customer_id").count()

result_df.write.mode("overwrite").parquet("s3://my-bucket/output/customer-order-counts/")
