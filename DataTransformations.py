from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
import os
import sys

# Setting environment variables for PySpark
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Initialize the Spark session
spark = SparkSession.builder \
        .appName("Data_Transformations") \
        .master("local[10]") \
        .getOrCreate()

# File path for the dataset
file_path = filepath

# Read the CSV file into a DataFrame
df = spark.read.csv(file_path, header=True, inferSchema=True)

# Calculate total sales value
df = df.withColumn("total_sales_value", col("sales") * col("price"))

# Add a column to indicate if discount was applied based on promo_bin_1 or promo_bin_2
df = df.withColumn("discount_applied", 
              when((col("promo_bin_1").isNotNull()) | (col("promo_bin_2").isNotNull()), True)
              .otherwise(False))

# Show the first 5 rows to verify the transformations
df.show(5)

# Drop unnecessary columns
df = df.drop("promo_type_1", "promo_type_2", "promo_bin_1", "promo_bin_2", "promo_discount_type_2")

# Show the first 5 rows after dropping columns
df.show(5)

# Renaming column (Make sure that the column exists or create it if needed)
# If `total_discount` exists, rename it. Otherwise, ensure it's created before renaming.
# Example: If you need to create total_discount
df = df.withColumn("total_discount", col("sales") * col("price") * 0.10)  # Example logic for discount
df = df.withColumnRenamed("total_discount", "promo_discount_2")

# Show the final DataFrame after renaming
df.show(5)
