# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "62e7a93e-9629-4847-987b-19bee835a349",
# META       "default_lakehouse_name": "WWI_Silver_Lakehouse",
# META       "default_lakehouse_workspace_id": "49c82d5a-db9c-4bc3-9c11-44ff611516ca",
# META       "known_lakehouses": [
# META         {
# META           "id": "4ef13e3e-c242-429f-a5d9-226944534855"
# META         },
# META         {
# META           "id": "62e7a93e-9629-4847-987b-19bee835a349"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

import re
from pyspark.sql.functions import current_timestamp, input_file_name

# 1. Define the source directory in OneLake
source_folder = "Files"

# 2. Retrieve all files using Microsoft Spark Utilities
files = mssparkutils.fs.ls(source_folder)

# Filter out only .csv files
csv_files = [f for f in files if f.name.endswith(".csv")]
total_files = len(csv_files)

print(f"Found {total_files} CSV files to process into Bronze Delta tables.")

# 3. Iterate through each CSV and save as a Bronze Delta Table
for index, file_info in enumerate(csv_files, start=1):
    raw_file_name = file_info.name
    file_path = file_info.path
    
    # Generate a clean Delta table name (e.g., 'Sales.Orders.csv' -> 'bronze_sales_orders')
    clean_name = raw_file_name.replace(".csv", "").replace(".", "_").lower()
    target_table_name = f"bronze_{clean_name}"
    
    print(f"[{index}/{total_files}] Processing: {raw_file_name} -> {target_table_name}...")
    
    try:
        # Load raw CSV as-is (strings preserved, no transformations)
        df_raw = spark.read.format("csv") \
            .option("header", "true") \
            .load(file_path)
        
        # Sanitize column names (replaces spaces, parentheses, etc. that Delta syntax rejects)
        sanitized_columns = [
            re.sub(r'[ ,;{}()\n\t=]', '_', col_name.strip()) for col_name in df_raw.columns
        ]
        df_clean_cols = df_raw.toDF(*sanitized_columns)
        
        # Optional: Add raw ingestion audit metadata
        df_bronze = df_clean_cols \
            .withColumn("_raw_file_source", input_file_name()) \
            .withColumn("_bronze_ingested_at", current_timestamp())
        
        # Write to Delta table in Lakehouse
        df_bronze.write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(target_table_name)
            
    except Exception as e:
        print(f"❌ Error processing {raw_file_name}: {str(e)}")

print("\n" + "="*50)
print(f"Completed! All Bronze Delta tables are now queryable in your Lakehouse.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# List all tables from the *current* default Lakehouse schema instead of a non-existent DB name
# In Fabric Lakehouses, spark.catalog.listTables() without arguments returns tables from the
# active database (usually the default Lakehouse schema, e.g. 'dbo').

# Optionally, you can inspect the current database if needed:
# print("Current database:", spark.catalog.currentDatabase())

# Get tables from the current database (no explicit DB name)
try:
    tables = spark.catalog.listTables()
except Exception as e:
    print("Error listing tables from current database:", str(e))
    raise

print("=== DATASET PROFILING RESULTS ===\n")

for table in tables:
    table_name = table.name
    # Fetch all columns for the current table from the current DB
    columns = spark.catalog.listColumns(table_name)
    col_names = [col.name.lower() for col in columns]
    
    # 1. Check if it's a Fact Table (Contains financial or measurable metrics)
    fact_keywords = ['price', 'amount', 'profit', 'quantity', 'tax']
    is_fact = any(keyword in name for name in col_names for keyword in fact_keywords)
    
    # 2. Check if it's a Dimension Table (Contains descriptive attributes)
    dim_keywords = ['name', 'description', 'city', 'color', 'category']
    is_dim = any(keyword in name for name in col_names for keyword in dim_keywords)
    
    # Print the assessment (using plain ASCII markers for reliability)
    if is_fact:
        print(f"FACT TABLE DETECTED: {table_name}")
        print("   -> Relevant for VP of Sales (Metrics & Money)")
    elif is_dim:
        print(f"DIMENSION DETECTED: {table_name}")
        print("   -> Relevant for BI Architect (Filtering & Grouping)")
    else:
        print(f"UNKNOWN GRAIN: {table_name}")
        
    print("-" * 40)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import current_timestamp, lit

# Define your environments
bronze_db = "WWI_Bronze_Lakehouse"
silver_db = "WWI_Silver_Lakehouse" 

# 1. Programmatically fetch all 51 tables from Bronze
tables = spark.catalog.listTables(bronze_db)

for table in tables:
    table_name = table.name
    
    # Read the raw table
    df_bronze = spark.read.table(f"{bronze_db}.{table_name}")
    
    # 2. Enforce the BI Architect's Deduplication Standard (Row-level)
    df_deduped = df_bronze.dropDuplicates()
    
    # 3. Enforce the BI Architect's Lineage Standard (Audit Metadata)
    df_silver = df_deduped \
        .withColumn("_silver_processed_at", current_timestamp()) \
        .withColumn("_source_system", lit(f"{bronze_db}.{table_name}"))
        
    # 4. Standardize the naming convention for the new layer
    silver_table_name = table_name.replace("bronze_", "silver_")
    
    # 5. Write to the Silver Lakehouse as Delta
    df_silver.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(f"{silver_db}.{silver_table_name}")
        
    print(f"✅ Successfully created and audited: {silver_table_name}")

print("\n🏁 Silver Layer fully provisioned.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
