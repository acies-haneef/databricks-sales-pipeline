from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Read source sales table
df = spark.table("workspace.default.sales")

# Write to Bronze table
(
    df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("workspace.default.bronze_sales")
)

print("Ingestion completed successfully.")
print(f"Records ingested: {df.count()}")
