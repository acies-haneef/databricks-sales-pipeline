from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.getOrCreate()

# Read Bronze table
df = spark.table("main.default.bronze_sales")

# Clean and transform
silver_df = (
    df
    .filter(col("order_id").isNotNull())
    .filter(col("customer_id").isNotNull())
    .filter(col("quantity") > 0)
    .filter(col("price") >= 0)
    .dropDuplicates(["order_id"])
    .withColumn(
        "total_amount",
        col("quantity") * col("price")
    )
)

# Write Silver table
(
    silver_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("main.default.silver_sales")
)

print("Silver transformation completed successfully.")
