from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number, rank
import os
import sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


# Initialize Spark session
spark = SparkSession.builder.appName("ETL_Pipeline").master("local[10]").getOrCreate()

# Step 1: Load the Raw Data
file_path = FilePath
raw_data = spark.read.csv(file_path, header=True, inferSchema=True)

print("\n✅ Loaded raw data:")
raw_data.show(5)

# Step 2: Data Cleaning
## 2.1: Checking for Missing Values in Key Attributes (product_id & store_id)
missing_values = raw_data.filter(col("product_id").isNull() | col("store_id").isNull())
if missing_values.count() > 0:
    print("\n Found missing values in product_id or store_id:")
    missing_values.show()
else:
    print("\n No missing values in product_id or store_id.")

## 2.2: Checking for Duplicate Records
duplicates = raw_data.groupBy("product_id", "store_id").count().filter(col("count") > 1)
if duplicates.count() > 0:
    print("\n Found duplicate records:")
    duplicates.show()
else:
    print("\n No duplicate records found.")

## 2.3: Applying Window Functions (row_number and rank)
window_spec = Window.orderBy(col("product_id").desc(), col("store_id").desc())
raw_data = raw_data.withColumn("row_number", row_number().over(window_spec)) \
                   .withColumn("rank", rank().over(window_spec))

# Step 3: Handling Missing Values in promo_bin_1 & promo_bin_2
raw_data = raw_data.fillna(0, ["promo_bin_1", "promo_bin_2"])
print("\n Filled null values in promo_bin_1 and promo_bin_2.")

# Step 4: Checking for Missing Values in sales, revenue, and stock
missing_values_critical = raw_data.filter(col("sales").isNull() | col("revenue").isNull() | col("stock").isNull())
if missing_values_critical.count() > 0:
    print("\n Found missing values in sales, revenue, or stock:")
    missing_values_critical.show()
else:
    print("\n No missing values in sales, revenue, or stock.")

# Step 5: Filling Null Values in sales, revenue, and stock with False for easy filtering.
raw_data = raw_data.fillna(False, ["sales", "revenue", "stock"])
print("\n Filled null values in sales, revenue, and stock.")

# Step 6: Persist Data (to improve performance if used multiple times)
raw_data.persist()

# Step 7: Final Check for Duplicates
final_duplicates = raw_data.groupBy("product_id", "store_id").count().filter(col("count") > 1)
if final_duplicates.count() > 0:
    print("\n Duplicate records still exist after cleaning:")
    final_duplicates.show()
else:
    print("\n No duplicate records found in the final dataset.")

# Step 8: Show final cleaned data
print("\n Final cleaned data:")
raw_data.show(5)
