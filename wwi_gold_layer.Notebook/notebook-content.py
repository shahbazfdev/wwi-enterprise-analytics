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

# CELL ********************

# MAGIC %%pyspark
# MAGIC # ============================================================================
# MAGIC # Lakehouse Fact Table Finder (PySpark)
# MAGIC # Lakehouse Fact Table Finder (PySpark)
# MAGIC # Loops through every Delta table in the current Lakehouse and scores it on
# MAGIC # heuristics that typically indicate a fact table:
# MAGIC #   - row count (facts are usually the biggest tables)
# MAGIC #   - number of ID/Key-suffixed columns (proxy for FK columns, since Delta
# MAGIC #     tables don't enforce real foreign keys)
# MAGIC #   - number of numeric/measure-like columns (facts are summable)
# MAGIC #   - presence of a date/timestamp column (events happen at a point in time)
# MAGIC #   - number of ID/Key columns >= 2 as a composite-key-ish signal
# MAGIC #
# MAGIC # Run this in a Fabric notebook attached to your Lakehouse.
# MAGIC # ============================================================================
# MAGIC 
# MAGIC from pyspark.sql.types import NumericType, DateType, TimestampType
# MAGIC 
# MAGIC results = []
# MAGIC 
# MAGIC # list every table registered in the lakehouse's default catalog
# MAGIC tables = spark.catalog.listTables()
# MAGIC 
# MAGIC for t in tables:
# MAGIC     table_name = t.name
# MAGIC     try:
# MAGIC         df = spark.table(table_name)
# MAGIC         schema = df.schema
# MAGIC 
# MAGIC         row_count = df.count()
# MAGIC 
# MAGIC         numeric_col_count = sum(1 for f in schema.fields if isinstance(f.dataType, NumericType))
# MAGIC         date_col_count = sum(
# MAGIC             1 for f in schema.fields
# MAGIC             if isinstance(f.dataType, (DateType, TimestampType))
# MAGIC         )
# MAGIC 
# MAGIC         # proxy for FK / composite key: columns ending in ID or Key (case-insensitive),
# MAGIC         # excluding the single most obvious primary id (first column ending in ID/Key
# MAGIC         # that also starts with the table's own name is usually the table's own PK,
# MAGIC         # but we keep this simple and just count all of them)
# MAGIC         id_like_cols = [
# MAGIC             f.name for f in schema.fields
# MAGIC             if f.name.lower().endswith("id") or f.name.lower().endswith("key")
# MAGIC         ]
# MAGIC         id_like_col_count = len(id_like_cols)
# MAGIC 
# MAGIC         has_composite_key_signal = 1 if id_like_col_count >= 2 else 0
# MAGIC 
# MAGIC         fact_score = (
# MAGIC             id_like_col_count * 3
# MAGIC             + numeric_col_count * 2
# MAGIC             + date_col_count * 2
# MAGIC             + has_composite_key_signal * 5
# MAGIC             + (3 if row_count > 10000 else 1 if row_count > 1000 else 0)
# MAGIC         )
# MAGIC 
# MAGIC         results.append({
# MAGIC             "table_name": table_name,
# MAGIC             "row_count": row_count,
# MAGIC             "id_like_col_count": id_like_col_count,
# MAGIC             "numeric_col_count": numeric_col_count,
# MAGIC             "date_col_count": date_col_count,
# MAGIC             "has_composite_key_signal": has_composite_key_signal,
# MAGIC             "fact_score": fact_score,
# MAGIC             "id_like_columns": ", ".join(id_like_cols)
# MAGIC         })
# MAGIC 
# MAGIC     except Exception as e:
# MAGIC         # skip anything that isn't a readable table (views, temp tables, etc.)
# MAGIC         results.append({
# MAGIC             "table_name": table_name,
# MAGIC             "row_count": None,
# MAGIC             "id_like_col_count": None,
# MAGIC             "numeric_col_count": None,
# MAGIC             "date_col_count": None,
# MAGIC             "has_composite_key_signal": None,
# MAGIC             "fact_score": None,
# MAGIC             "id_like_columns": f"ERROR: {str(e)[:100]}"
# MAGIC         })
# MAGIC 
# MAGIC result_df = spark.createDataFrame(results)
# MAGIC result_df = result_df.orderBy(result_df.fact_score.desc())
# MAGIC 
# MAGIC display(result_df)
# MAGIC 
# MAGIC # Optional: write the ranking itself to a Delta table so you have a record
# MAGIC # of your modeling decisions (nice touch for your project write-up / Phase 7)
# MAGIC # result_df.write.mode("overwrite").saveAsTable("silver_fact_dimension_scan")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
