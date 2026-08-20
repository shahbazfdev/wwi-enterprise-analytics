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
# META           "id": "62e7a93e-9629-4847-987b-19bee835a349"
# META         }
# META       ]
# META     },
# META     "warehouse": {
# META       "default_warehouse": "9efe4108-02cd-b336-4888-d8c74277c3ba",
# META       "known_warehouses": [
# META         {
# META           "id": "9efe4108-02cd-b336-4888-d8c74277c3ba",
# META           "type": "Datawarehouse"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

print("🚀 Hunting for the Fact Table: Profiling all tables...")

# 1. Get all tables in the current environment
tables_df = spark.sql("SHOW TABLES")
table_names = [row.tableName for row in tables_df.collect()]

fact_candidates = []

for table in table_names:
    # Skip the archive tables to save processing time; they are historical logs
    if "archive" in table.lower():
        continue
        
    df = spark.read.table(table)
    columns = df.columns
    
    # Logic 1: Count the Foreign Keys (Connecting to Dimensions)
    id_columns = [c for c in columns if c.endswith("ID")]
    
    # Logic 2: Count the Measures (The Math)
    measure_keywords = ["price", "amount", "profit", "tax", "quantity", "cost", "discount"]
    measure_columns = [c for c in columns if any(kw in c.lower() for kw in measure_keywords)]
    
    # Logic 3: Get the Volume (Events happen frequently)
    row_count = df.count()
    
    # Calculate a simple "Fact Score"
    score = len(id_columns) + len(measure_columns)
    
    # Filter: A true Fact table must have at least some IDs and some math
    if len(id_columns) >= 2 and len(measure_columns) >= 1:
        fact_candidates.append({
            "TableName": table,
            "ForeignKeys_Count": len(id_columns),
            "Measures_Count": len(measure_columns),
            "RowCount": row_count,
            "FactScore": score
        })

# Sort by FactScore and RowCount to rank the best candidates
fact_candidates = sorted(fact_candidates, key=lambda x: (x["FactScore"], x["RowCount"]), reverse=True)

# Display the ranked results
df_results = spark.createDataFrame(fact_candidates)
print("\n🎯 Top Fact Table Candidates Ranked by Signature:")
display(df_results)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 1. Inspect the Line Items (The granular math)
df_lines = spark.read.table("silver_sales_invoicelines")
print("=== FACT CANDIDATE 1: silver_sales_invoicelines ===")
display(df_lines.limit(10))

# 2. Inspect the Invoice Headers (The transactional context)
df_headers = spark.read.table("silver_sales_invoices")
print("\n=== FACT CANDIDATE 2: silver_sales_invoices ===")
display(df_headers.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
