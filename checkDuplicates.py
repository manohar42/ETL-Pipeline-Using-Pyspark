from pyspark.sql.functions import col, row_number, rank
from pyspark.sql.window import Window
class checkDuplicates:

    def __init__(self,spark):
        self.spark = spark

    def checkForDuplicates(self,df):
        return df.groupby("product_id","store_id").count().filter(col("count")>1)

    def handling_duplicates(self,df):
    # Applying Window Functions(row_number and rank)
        window_spec = Window.orderBy(col("product_id").desc(), col("store_id").desc())
        return df.withColumn("row_number", row_number().over(window_spec)) \
                           .withColumn("rank", rank().over(window_spec))
