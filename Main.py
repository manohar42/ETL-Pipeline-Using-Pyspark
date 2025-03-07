from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from LoadData import LoadData
from dataCleaning import dataCleaning


class Spark:

    def __init__(self):
        self.conf = SparkConf()
        self.conf.set("app.name","ETLPipeline") \
                .set("master","local[2]")
        self.spark = SparkSession.builder \
                .config(conf=self.conf) \
                .getOrCreate()
    def loadData(self):
        self.data = LoadData(self.spark).return_data()
        self.data.show()
        print("\n✅ Loaded raw data:")

    def data_cleaning(self):
        dataClean = dataCleaning(self.spark)
        missing_values = dataClean.check_missing_values(self.data)
        if missing_values.count() > 0:
            print("\n⚠️ Found missing values in product_id or store_id:")
            missing_values.show()
        else:
            print("\n✅ No missing values in product_id or store_id.")






if __name__ == "__main__":
    s = Spark()
    # Step 1: Load the Raw Data
    s.loadData()
    # Step 2: Data Cleaning
    ## 2.1: Checking for Missing Values in Key Attributes (product_id & store_id)
    s.data_cleaning()
