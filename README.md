# ETL Pipeline Using PySpark

## Project Overview
This project is an **ETL (Extract, Transform, Load) Pipeline** implemented using **PySpark**. It processes data by detecting duplicates, performing data cleaning, and loading the final dataset.

## Prerequisites
Ensure the following dependencies are installed before running the project:
- Python 3.7+
- PySpark (`pip install pyspark`)
- Other dependencies (if required, such as pandas, NumPy)

## Project Structure
```
ETL-Pipeline-Using-Pyspark/
│── Main.py               # Entry point for the ETL pipeline
│── LoadData.py           # Handles data ingestion from a CSV file
│── DataCleaning.py       # Cleans and transforms the dataset
│── checkDuplicates.py    # Detects and resolves duplicate records
│── README.md             # Documentation
```

## Usage Instructions
To run the ETL pipeline, use the following command:
```sh
python Main.py
```

## Spark Configuration
The project initializes a PySpark session with the following settings:
```python
spark = SparkSession.builder.appName("ETL_Pipeline").master("local[10]").getOrCreate()
```
By default, it runs in **local mode** but can be configured for a distributed environment.

## Functionality Overview
1. **Data Loading** (`LoadData.py`):
   - Reads data from a CSV file into a PySpark DataFrame.

2. **Duplicate Handling** (`checkDuplicates.py`):
   - Uses **groupBy** and **Window functions** to check for duplicates.
   - Applies ranking to resolve duplicate issues.

3. **Data Cleaning** (`DataCleaning.py`):
   - Performs necessary transformations for data consistency.

4. **ETL Execution** (`Main.py`):
   - Calls all modules and runs the full pipeline.

## Output
The final processed data is stored in a specified format (CSV/Parquet).

---
**Author**: Manohar Veeravalli 
**License**: MIT  
