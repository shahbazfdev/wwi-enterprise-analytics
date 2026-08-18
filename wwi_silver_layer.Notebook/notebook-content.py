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

from pyspark.sql.functions import current_timestamp, lit

# In Fabric, use the built-in `notebookutils.fs` instead of `mssparkutils`

# 1. Base ABFSS path to the Bronze Lakehouse *Tables* root
bronze_abfss_base = "abfss://WWI_Analytics_Dev@onelake.dfs.fabric.microsoft.com/WWI_Bronze_Lakehouse.Lakehouse/Tables"

print(f"🚀 Scanning physical OneLake path: {bronze_abfss_base}")

# 2. Detect schema folder (e.g. `dbo`) and drill into it before listing tables
try:
    root_dirs = notebookutils.fs.ls(bronze_abfss_base)
    schema_dirs = [d for d in root_dirs if d.isDir]

    # If there is a `dbo` folder, assume schema-enabled lakehouse and use it as the table root
    dbo_dir = next((d for d in schema_dirs if d.name == "dbo"), None)
    if dbo_dir is not None:
        bronze_tables_root = f"{bronze_abfss_base}/dbo"
        print("📂 Detected schema-enabled lakehouse. Using 'dbo' as table root.")
    else:
        bronze_tables_root = bronze_abfss_base
        print("📂 No schema folder detected. Using Tables root directly.")

    table_dirs = notebookutils.fs.ls(bronze_tables_root)
    table_names = [file.name for file in table_dirs if file.isDir]
    print(f"📦 Found {len(table_names)} physical Delta tables on disk under {bronze_tables_root}.\n")
except Exception as e:
    print(f"❌ Storage connection failed. Error: {e}")
    table_names = []
    bronze_tables_root = bronze_abfss_base

# 3. Iterate, transform, and write to default (Silver) lakehouse
for table in table_names:
    # Read directly from the physical Delta path inside the schema (e.g. /Tables/dbo/<table>)
    bronze_table_path = f"{bronze_tables_root}/{table}"
    print(f"🔎 Reading Bronze table from: {bronze_table_path}")
    df_bronze = spark.read.format("delta").load(bronze_table_path)

    # Apply standard deduplication and audit lineage
    df_silver = (
        df_bronze.dropDuplicates()
        .withColumn("_silver_processed_at", current_timestamp())
        .withColumn("_source_system", lit(f"abfss_bronze:{table}"))
    )

    # Derive Silver table name
    if table.startswith("bronze_"):
        silver_table_name = "silver_" + table[len("bronze_"):]
    else:
        silver_table_name = "silver_" + table

    # Write to the default Lakehouse (Silver) using standard catalog syntax
    df_silver.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(silver_table_name)

    print(f"✅ Extracted via ABFSS and saved to Silver: {silver_table_name}")

print("\n🏁 Silver Layer fully provisioned.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("=== ACTIVE NAMESPACE ===")
print(spark.catalog.currentDatabase())

print("\n=== AVAILABLE DATABASES (LAKEHOUSES) ===")
display(spark.sql("SHOW DATABASES"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM WWI_Bronze_Lakehouse.dbo.bronze_application_cities LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
