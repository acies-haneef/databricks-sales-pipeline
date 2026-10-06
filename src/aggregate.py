from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, count

spark = SparkSession.builder.getOrCreate()

# Read Silver table
df = spark.table("workspace.default.silver_sales")

# Create business-level aggregation
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
    .saveAsTable("workspace.default.gold_product_sales")
)

print("Aggregation completed successfully.")
print(f"Products processed: {gold_df.count()}")
