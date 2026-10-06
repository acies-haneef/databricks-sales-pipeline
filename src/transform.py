from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date

spark = SparkSession.builder.getOrCreate()

# Read Bronze table
df = spark.table("workspace.default.bronze_sales")

# Clean and transform the data
silver_df = (
    df
    .filter(col("order_id").isNotNull())
    .filter(col("customer_id").isNotNull())
    .filter(col("product_id").isNotNull())
    .filter(col("quantity") > 0)
    .filter(col("price") >= 0)
    .dropDuplicates(["order_id"])
    .withColumn("order_date", to_date(col("order_date")))
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
    .saveAsTable("workspace.default.silver_sales")
)

print("Transformation completed successfully.")
print(f"Records after transformation: {silver_df.count()}")
