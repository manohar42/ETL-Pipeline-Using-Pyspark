from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from LoadData import LoadData
from checkDuplicates import checkDuplicates
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
        missing_values = dataClean.check_missing_product_store_id_data(self.data)
        if missing_values.count() > 0:
            print("\n⚠️ Found missing values in product_id or store_id:")
            missing_values.show()
            print("Handling missing values")
            self.data = missing_values.dropna(how='any')
        else:
            print("\n✅ No missing values in product_id or store_id.")

        missing_sales_data = dataClean.check_missing_sales_revenue_stock_data(self.data)
        if missing_sales_data.count() > 0:
            print("\n⚠️ Found missing values in sales, revenue, or stock:")
            missing_sales_data.show()
            print("Handling missing values")
            self.data = self.data.fillna(0,["sales","revenue","stock"])
            print("Handled missing values")
            print("Rechecking for Missing Values")
            missing_sales_data = dataClean.check_missing_sales_revenue_stock_data(self.data)
            if missing_sales_data.count() > 0:
                print("missing_values not handled")
            else:
                print("\n✅ No missing values in sales, revenue, or stock.")
        else:
            print("\n✅ No missing values in sales, revenue, or stock.")

    def check_duplicates(self):

        duplicatechecksession = checkDuplicates(self.spark)

        duplicate_values = duplicatechecksession.checkForDuplicates(self.data)
        if duplicate_values.count() > 0:
            print("\n⚠️ Found duplicate records:")
            duplicate_values.show()
            print("Handling duplicate records")
            self.data = duplicatechecksession.handling_duplicates(self.data)

        else:
            print("\n✅ No duplicate records found.")













if __name__ == "__main__":
    s = Spark()
    # Step 1: Load the Raw Data
    s.loadData()
    # Step 2: Data Cleaning
    ## 2.1: Checking for Missing Values in Key Attributes (product_id & store_id)
    s.data_cleaning()
    ## 2.2: Checking for Duplicate Records
    s.check_duplicates()

