from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Source data stored in Unity Catalog Volume
source_path = "/Volumes/workspace/default/sales_data/sales.csv"

# Read raw sales data
df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(source_path)
)

# Write Bronze table
(
    df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("workspace.default.bronze_sales")
)

print("Ingestion completed successfully.")
print(f"Records ingested: {df.count()}")
