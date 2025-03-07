class LoadData:

    def __init__(self,spark):
        self.file_path = filepath
        self.spark = spark

    def return_data(self):
        return self.spark.read.csv(self.file_path, header=True, inferSchema=True)
