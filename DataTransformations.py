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

df = df.withColumn("total_sales_value", col("sales") * col("price"))


df = df.withColumn("discount_applied", 
              when((col("promo_bin_1").isNotNull()) | (col("promo_bin_2").isNotNull()), True)
              .otherwise(False))


df.show(5)

df = df.drop("promo_type_1", "promo_type_2", "promo_bin_1", "promo_bin_2", "promo_discount_type_2")


df.show(5)


df = df.withColumn("total_discount", col("sales") * col("price") * 0.10)  
df = df.withColumnRenamed("total_discount", "promo_discount_2")


df.show(5)
