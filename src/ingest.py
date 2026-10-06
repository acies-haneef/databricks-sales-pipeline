from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Read raw sales data
df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("/Volumes/main/default/sales_data/sales.csv")
)

# Write Bronze table
(
    df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("main.default.bronze_sales")
)

print("Bronze ingestion completed successfully.")
