# gold_layer.py
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, count, col

def run_gold_aggregation(input_table, output_table):
    spark = SparkSession.builder.appName("GoldAggregation").getOrCreate()
    
    # 1. Read from Silver Delta Table
    print(f"Reading from Silver Delta table: {input_table}")
    silver_df = spark.table(input_table)
    
    # 2. Apply Final Aggregations (Example: total sales per customer)
    gold_df = silver_df.groupBy(
        col("customer_name_cleaned")
    ).agg(
        sum("total_order_value").alias("total_lifetime_sales"),
        count("id").alias("total_orders_placed")
    ).sort(
        col("total_lifetime_sales").desc()
    )
    
    # 3. Write to Gold Delta Table
    # The 'gold' schema/database must exist
    print(f"Writing to Gold Delta table: {output_table}")
    gold_df.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(output_table)
        
    print("Gold aggregation complete. Data ready for BI.")

if __name__ == "__main__":
    # The parameters are parsed from the job JSON
    if len(sys.argv) != 5:
        print("Usage: python gold_layer.py --input-table <silver_table> --output-table <gold_table>")
        sys.exit(1)
        
    input_table = sys.argv[2]
    output_table = sys.argv[4]
    
    run_gold_aggregation(input_table, output_table)