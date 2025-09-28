# bronze_layer.py
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit

def run_bronze_ingestion(input_path, output_table):
    spark = SparkSession.builder.appName("BronzeIngestion").getOrCreate()
    
    # 1. Read Raw Data (Assuming a CSV in a mounted or configured cloud path)
    print(f"Reading raw data from: {input_path}")
    df = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(input_path)
    
    # 2. Add Audit Columns
    bronze_df = df.withColumn("ingestion_timestamp", current_timestamp()) \
                  .withColumn("source_file", lit(input_path))

    # 3. Write to Bronze Delta Table
    # The 'bronze' schema/database must exist in Unity Catalog or Hive Metastore
    print(f"Writing to Bronze Delta table: {output_table}")
    bronze_df.write.format("delta") \
        .mode("overwrite") \
        .saveAsTable(output_table)
    
    print("Bronze ingestion complete.")

if __name__ == "__main__":
    # The parameters are parsed from the job JSON (the "parameters" array)
    if len(sys.argv) != 5:
        print("Usage: python bronze_layer.py --input-path <path> --output-table <table_name>")
        sys.exit(1)
        
    # Basic argument parsing (can be made more robust with argparse)
    input_path = sys.argv[2]
    output_table = sys.argv[4]
    
    run_bronze_ingestion(input_path, output_table)