# silver_layer.py
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper, regexp_replace

def run_silver_transformation(input_table, output_table):
    spark = SparkSession.builder.appName("SilverTransformation").getOrCreate()
    
    # 1. Read from Bronze Delta Table
    print(f"Reading from Bronze Delta table: {input_table}")
    bronze_df = spark.table(input_table)
    
    # 2. Apply Transformations and Cleaning (Example logic)
    silver_df = bronze_df.select(
        col("id"),
        upper(col("customer_name")).alias("customer_name_cleaned"),
        col("order_value").cast("double").alias("total_order_value"),
        col("ingestion_timestamp")
    ).filter(
        # Example data quality filter: ensure order value is positive
        col("total_order_value").isNotNull() & (col("total_order_value") > 0)
    )
    
    # 3. Write to Silver Delta Table
    # The 'silver' schema/database must exist
    print(f"Writing to Silver Delta table: {output_table}")
    silver_df.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(output_table)
        
    print("Silver transformation complete.")

if __name__ == "__main__":
    # The parameters are parsed from the job JSON
    if len(sys.argv) != 5:
        print("Usage: python silver_layer.py --input-table <bronze_table> --output-table <silver_table>")
        sys.exit(1)
        
    input_table = sys.argv[2]
    output_table = sys.argv[4]
    
    run_silver_transformation(input_table, output_table)