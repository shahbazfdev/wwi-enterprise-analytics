# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "4ef13e3e-c242-429f-a5d9-226944534855",
# META       "default_lakehouse_name": "WWI_Bronze_Lakehouse",
# META       "default_lakehouse_workspace_id": "49c82d5a-db9c-4bc3-9c11-44ff611516ca",
# META       "known_lakehouses": [
# META         {
# META           "id": "4ef13e3e-c242-429f-a5d9-226944534855"
# META         },
# META         {
# META           "id": "8b1fddab-7f1d-4807-b397-3b7b3b70f8a1"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

import pandas as pd

# Tell Fabric where your Excel file is in the Lakehouse Files section
# (Adjust the path if it's in a subfolder, e.g., "Files/Bronze/Meta_ForeignKeys.xlsx")
file_path = "abfss://WWI_Analytics_Dev@onelake.dfs.fabric.microsoft.com/WWI_Bronze_Lakehouse.Lakehouse/Files/Bronze.Meta_ForeignKeys.xlsx"  

# Read the Excel file using Pandas
try:
    pdf_meta = pd.read_excel(file_path)
    # Convert Pandas DataFrame to Spark DataFrame
    fk_meta = spark.createDataFrame(pdf_meta)
    
    print("✅ Metadata loaded successfully!")
    print(f"Total columns: {len(fk_meta.columns)}")
    print("Column names:", fk_meta.columns)
    
    # Show the first 5 rows so we know what the columns are actually named
    display(fk_meta)
    
except FileNotFoundError:
    print(f"❌ File not found at '{file_path}'")
    print("Please check the exact path in your Lakehouse Files section.")
    print("Try changing the path to: Files/YourFolderName/Meta_ForeignKeys.xlsx")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, count, desc

# Count how many times each table appears as a "child" (Fact)
fact_candidates = fk_meta.groupBy("Fact_Table_Name") \
                         .agg(count("FK_Column").alias("FK_Count")) \
                         .orderBy(desc("FK_Count"))

# Show the top 5 candidates
display(fact_candidates.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pandas as pd

# Read the Excel file
file_path = "abfss://WWI_Analytics_Dev@onelake.dfs.fabric.microsoft.com/WWI_Bronze_Lakehouse.Lakehouse/Files/Bronze.Meta_ForeignKeys.xlsx"  
pdf_meta = pd.read_excel(file_path)

# Show the exact column names
print("--- EXACT COLUMN NAMES IN YOUR EXCEL ---")
print(pdf_meta.columns.tolist())

# Show the first row so we see how the data is structured
print("\n--- FIRST ROW OF DATA (SAMPLE) ---")
print(pdf_meta.iloc[0].to_dict())

# Show the top 5 rows in a nice table
display(pdf_meta.head(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, count, desc

# List of audit columns that skew the results
audit_columns = ['LastEditedBy', 'ValidFrom', 'ValidTo', 'EditedBy']

# Filter them out
filtered_meta = fk_meta.filter(~col("FK_Column").isin(audit_columns))

# Now count which tables have the most BUSINESS foreign keys
fact_candidates = filtered_meta.groupBy("Fact_Table_Name") \
                               .agg(count("FK_Column").alias("Business_FK_Count")) \
                               .orderBy(desc("Business_FK_Count"))

print("--- TOP 10 TABLES WITH BUSINESS FOREIGN KEYS ---")
display(fact_candidates.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Explicitly look for OrderLines and PurchaseOrderLines in the metadata
target_facts = ['OrderLines', 'PurchaseOrderLines']

for fact in target_facts:
    print(f"\n--- Searching for: {fact} ---")
    # Find all direct parents of this table
    parents = fk_meta.filter(col("Fact_Table_Name") == fact) \
                     .select("FK_Column", "Dimension_Table_Name", "PK_Column") \
                     .distinct() \
                     .collect()
    
    if parents:
        print(f"✅ Found {fact} with {len(parents)} direct Foreign Keys:")
        for row in parents:
            print(f"   - {fact}.{row.FK_Column} -> {row.Dimension_Table_Name}.{row.PK_Column}")
    else:
        print(f"❌ {fact} not found in metadata. Check the exact spelling (case sensitive?).")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col

# Audit columns to skip (these cause loops and are not needed for business logic)
audit_columns = ['LastEditedBy', 'ValidFrom', 'ValidTo', 'EditedBy']

def get_related_tables(start_table, fk_meta_df, depth=0, max_depth=10):
    """
    Recursively finds all parent tables needed for a given fact.
    Skips audit columns to avoid infinite loops.
    """
    if depth > max_depth:
        return set()
    
    visited = set()
    tables_to_process = [start_table]
    all_needed = set([start_table])
    
    while tables_to_process:
        current = tables_to_process.pop()
        if current in visited:
            continue
        visited.add(current)
        
        # Find direct parents of this table, BUT skip audit columns
        parents = fk_meta_df.filter(
            (col("Fact_Table_Name") == current) & 
            (~col("FK_Column").isin(audit_columns))
        ).select("Dimension_Table_Name").distinct().collect()
        
        for row in parents:
            parent = row.Dimension_Table_Name
            if parent not in visited:
                all_needed.add(parent)
                tables_to_process.append(parent)  # Now check the parent's parents
    
    return all_needed

# Get all tables needed for Sales (starting from OrderLines)
sales_tables = get_related_tables("OrderLines", fk_meta)
print("--- TABLES NEEDED FOR SALES FACT (OrderLines) ---")
sales_list = sorted(sales_tables)
print(sales_list)

# Get all tables needed for Purchasing (starting from PurchaseOrderLines)
purchase_tables = get_related_tables("PurchaseOrderLines", fk_meta)
print("\n--- TABLES NEEDED FOR PURCHASING FACT (PurchaseOrderLines) ---")
purchase_list = sorted(purchase_tables)
print(purchase_list)

# Combine both lists to get your final Silver staging list
all_silver_tables = sales_tables.union(purchase_tables)
print("\n--- MASTER LIST: ALL TABLES TO COPY TO SILVER (DEDUPLICATED) ---")
master_list = sorted(all_silver_tables)
print(master_list)
print(f"\nTotal tables to clean: {len(master_list)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Replace 'Silver_Lakehouse' with the EXACT name you gave your Silver Lakehouse
SILVER_LAKEHOUSE_NAME = "WWI_Silver_Lakehouse" 

try:
    # Get the ABFS path of the Silver Lakehouse
    silver_lakehouse_info = mssparkutils.lakehouse.get(SILVER_LAKEHOUSE_NAME)
    SILVER_ABFS_PATH = silver_lakehouse_info.properties['abfsPath']
    print(f"✅ Silver Lakehouse found!")
    print(f"   ABFS Path: {SILVER_ABFS_PATH}")
except Exception as e:
    print(f"❌ Could not find Lakehouse named '{SILVER_LAKEHOUSE_NAME}'.")
    print("   Make sure you have attached it to this notebook (click the Lakehouse icon on the top left).")
    print(f"   Error: {e}")
# Master list from your metadata


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

master_list = ['BuyingGroups', 'Cities', 'Colors', 'Countries', 'CustomerCategories', 
               'Customers', 'DeliveryMethods', 'OrderLines', 'Orders', 'PackageTypes', 
               'People', 'PurchaseOrderLines', 'PurchaseOrders', 'StateProvinces', 
               'StockItems', 'SupplierCategories', 'Suppliers']

audit_columns_to_drop = ['ValidFrom', 'ValidTo', 'LastEditedBy', 'EditedBy', 'RowNumber', 'RowNum']

print("🚀 Starting Silver ingestion to EXACT ABFS path:")
print(f"   {SILVER_ABFS_PATH}/Tables/")
print("=" * 60)

for table_name in master_list:
    try:
        print(f"\n📥 Processing: {table_name}")
        
        # 1. Read from Bronze (default attached Lakehouse)
        df = spark.table(f"Bronze.{table_name}")
        original_count = df.count()
        print(f"   - Original rows in Bronze: {original_count}")
        
        # 2. Basic Cleaning
        df_cleaned = df.dropna(how="all")
        
        existing_cols = df_cleaned.columns
        cols_to_drop = [c for c in audit_columns_to_drop if c in existing_cols]
        if cols_to_drop:
            df_cleaned = df_cleaned.drop(*cols_to_drop)
            print(f"   - Dropped columns: {cols_to_drop}")
        
        # 3. Write to the EXPLICIT ABFS PATH (OneLake)
        target_path = f"{SILVER_ABFS_PATH}/Tables/{table_name}"
        df_cleaned.write.mode("overwrite").format("delta").save(target_path)
        
        # 4. (Optional) Register this table in the Spark catalog so you can query it immediately
        spark.sql(f"CREATE OR REPLACE TEMP VIEW Silver_{table_name} USING DELTA LOCATION '{target_path}'")
        
        print(f"   ✅ Silver.{table_name} created at: {target_path}")
        print(f"   - New rows: {df_cleaned.count()}")
        
    except Exception as e:
        print(f"   ❌ ERROR processing {table_name}: {str(e)}")

print("\n" + "=" * 60)
print("✅ PHASE 1 COMPLETE: All 17 tables written to Silver Lakehouse via ABFS path.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 👇 CHANGE THIS TO THE EXACT NAME OF YOUR BRONZE LAKEHOUSE
BRONZE_LAKEHOUSE_NAME = "WWI_Bronze_Lakehouse"  # <-- Adjust if different

print("🔍 Inspecting Bronze Lakehouse...")
print("=" * 60)

try:
    # 1. Get ABFS path for Bronze
    bronze_info = mssparkutils.lakehouse.get(BRONZE_LAKEHOUSE_NAME)
    bronze_path = bronze_info.properties['abfsPath']
    print(f"✅ Bronze Lakehouse found at:\n   {bronze_path}\n")
    
    # 2. List the "Tables" folder (where Delta tables usually live)
    tables_folder = f"{bronze_path}/Tables/"
    print(f"📁 Contents of '{tables_folder}':")
    try:
        tables_list = mssparkutils.fs.ls(tables_folder)
        if tables_list:
            for item in tables_list:
                if item.isDir:
                    print(f"   [DIR] {item.name}")
                else:
                    print(f"   [FILE] {item.name}")
        else:
            print("   (empty or no Tables folder)")
    except Exception as e:
        print(f"   ❌ Could not list Tables folder: {e}")
    
    # 3. List the "Files" folder (where raw CSVs/Excel might live)
    files_folder = f"{bronze_path}/Files/"
    print(f"\n📁 Contents of '{files_folder}':")
    try:
        files_list = mssparkutils.fs.ls(files_folder)
        if files_list:
            for item in files_list:
                if item.isDir:
                    print(f"   [DIR] {item.name}")
                    # List inside subfolders (to see CSVs)
                    sub_items = mssparkutils.fs.ls(item.path)
                    for sub in sub_items:
                        print(f"      [FILE] {sub.name}")
                else:
                    print(f"   [FILE] {item.name}")
        else:
            print("   (empty or no Files folder)")
    except Exception as e:
        print(f"   ❌ Could not list Files folder: {e}")
        
except Exception as e:
    print(f"❌ Could not find Lakehouse named '{BRONZE_LAKEHOUSE_NAME}'.")
    print("   Please check the name and make sure the Lakehouse is attached to this notebook.")
    print(f"   Error: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# This assumes SILVER_ABFS_PATH is already defined from earlier.
# If not, uncomment and run this:
# SILVER_LAKEHOUSE_NAME = "Silver_Lakehouse"
# silver_info = mssparkutils.lakehouse.get(SILVER_LAKEHOUSE_NAME)
# SILVER_ABFS_PATH = silver_info.properties['abfsPath']

# Get Bronze ABFS path (use your exact Bronze Lakehouse name)
BRONZE_LAKEHOUSE_NAME = "WWI_Bronze_Lakehouse"
bronze_info = mssparkutils.lakehouse.get(BRONZE_LAKEHOUSE_NAME)
BRONZE_ABFS_PATH = bronze_info.properties['abfsPath']

# List all CSV files in the Bronze Files folder
files_path = f"{BRONZE_ABFS_PATH}/Files/"
all_files = mssparkutils.fs.ls(files_path)
csv_files = [f.name for f in all_files if f.name.endswith('.csv') and not f.name.endswith('_Archive.csv')]

# Build mapping: TableName -> CSV file path
table_to_file = {}
for csv in csv_files:
    parts = csv.replace('.csv', '').split('.')
    if len(parts) >= 2:
        table_name = parts[-1]
        if table_name not in table_to_file:
            table_to_file[table_name] = f"{files_path}{csv}"
    else:
        table_name = parts[0]
        if table_name not in table_to_file:
            table_to_file[table_name] = f"{files_path}{csv}"

print(f"Found {len(table_to_file)} table mappings.")

# Your master list from metadata
master_list = ['BuyingGroups', 'Cities', 'Colors', 'Countries', 'CustomerCategories', 
               'Customers', 'DeliveryMethods', 'OrderLines', 'Orders', 'PackageTypes', 
               'People', 'PurchaseOrderLines', 'PurchaseOrders', 'StateProvinces', 
               'StockItems', 'SupplierCategories', 'Suppliers']

audit_columns_to_drop = ['ValidFrom', 'ValidTo', 'LastEditedBy', 'EditedBy', 'RowNumber', 'RowNum']

print("\n🚀 Starting Silver ingestion from CSVs...")
print(f"   Silver ABFS path: {SILVER_ABFS_PATH}/Tables/")
print("=" * 60)

success_count = 0
failures = []

for table_name in master_list:
    try:
        # 1. Find the CSV file for this table
        csv_path = table_to_file.get(table_name)
        if not csv_path:
            print(f"⚠️ No CSV found for {table_name}, skipping.")
            continue
        
        print(f"\n📥 Processing: {table_name} from {csv_path}")
        
        # 2. Read CSV with header and schema inference
        # PERMISSIVE mode handles corrupted records gracefully
        df = spark.read.option("header", "true") \
                       .option("inferSchema", "true") \
                       .option("mode", "PERMISSIVE") \
                       .csv(csv_path)
        
        original_count = df.count()
        print(f"   - Original rows in CSV: {original_count}")
        
        # 3. Basic Cleaning: Drop empty rows and audit columns
        df_cleaned = df.dropna(how="all")
        
        existing_cols = df_cleaned.columns
        cols_to_drop = [c for c in audit_columns_to_drop if c in existing_cols]
        if cols_to_drop:
            df_cleaned = df_cleaned.drop(*cols_to_drop)
            print(f"   - Dropped columns: {cols_to_drop}")
        
        # 4. Write to Silver using Delta format (OVERWRITE)
        target_path = f"{SILVER_ABFS_PATH}/Tables/{table_name}"
        df_cleaned.write.mode("overwrite").format("delta").save(target_path)
        
        new_count = df_cleaned.count()
        print(f"   ✅ Silver.{table_name} created with {new_count} rows")
        print(f"   📍 Location: {target_path}")
        success_count += 1
        
    except Exception as e:
        print(f"   ❌ ERROR processing {table_name}: {str(e)}")
        failures.append(table_name)

print("\n" + "=" * 60)
print(f"✅ PHASE 1 COMPLETE: {success_count} out of {len(master_list)} tables processed.")
if failures:
    print(f"⚠️ Failed tables: {failures}")
else:
    print("🎉 All 17 tables successfully ingested into Silver Lakehouse!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Verify that Silver Delta files exist
silver_tables_path = f"{SILVER_ABFS_PATH}/Tables/"
print(f"🔍 Checking Silver tables at: {silver_tables_path}")
silver_dirs = mssparkutils.fs.ls(silver_tables_path)
print("\n📁 Tables in Silver Lakehouse:")
for item in silver_dirs:
    if item.isDir:
        print(f"  - {item.name}")

# Read one table directly from Delta to confirm
print("\n📊 Sample from Silver.OrderLines:")
df_sample = spark.read.format("delta").load(f"{SILVER_ABFS_PATH}/Tables/OrderLines")
display(df_sample.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
