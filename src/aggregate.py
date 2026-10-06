from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, count

spark = SparkSession.builder.getOrCreate()

# Read Silver table
df = spark.table("main.default.silver_sales")

# Aggregate sales by product
gold_df = (
    df.groupBy("product_id")
      .agg(
          sum("total_amount").alias("total_sales"),
          count("order_id").alias("order_count")
      )
)

# Write Gold table
(
    gold_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("main.default.gold_product_sales")
)

print("Gold aggregation completed successfully.")
