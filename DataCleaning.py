from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os
import sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
if __name__ == "__main__":

    spark = SparkSession.builder.appName("ETLPipeline").master("local[10]").getOrCreate()

    raw_data = spark.read.csv(file_Path,header = True,inferSchema=True)
    raw_data.show()
    #Loaded the RAW data
    
# Data Cleaning

    # Checking if there are any missing values in attributes like product_id and store_id
    raw_data.filter(col("product_id").isNull() | col("store_id").isNull()).show()
    # Returned zero rows which indicated there are no null values for product_id and store_id


    # Checking if there are null values for promo_bin_1 and promo_bin_2 attributes
    RecordswithNullValues = raw_data.filter(col("promo_bin_1").isNull() | col("promo_bin_2").isNull())


    #Replacing the null values with zero.

    Updated_records = RecordswithNullValues.fillna(0,["promo_bin_1","promo_bin_2"])
    # Updated_records.show()

    Updated_records.filter(col("sales").isNull() | col("revenue").isNull() | col("stock").isNull()).show()

    Updated_records_final = Updated_records.fillna(0,["sales","stock","revenue"])

    # Updated_records_final.show()
    
    # Checking for duplicates in the data.
    Updated_records.groupBy("product_id","store_id").count().filter(col("count")>1).show()
    input("Press Enter:")




