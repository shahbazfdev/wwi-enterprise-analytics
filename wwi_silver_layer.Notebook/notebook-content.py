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

# CELL ********************

# Read the newly created Silver table
df_check = spark.read.table("silver_sales_invoicelines")

# Print the schema to see the column list
print("=== SCHEMA CHECK ===")
df_check.printSchema()

# Select only the metadata columns and 1 identifier to prove the row-level stamp worked
print("\n=== DATA CHECK ===")
display(df_check.select("InvoiceLineID", "_silver_processed_at", "_source_system").limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col

# Load a specific Silver table you want to cast
# TODO: Replace 'silver_sales_invoicelines' with the actual Silver table name you are working with
# Example: df_silver = spark.read.table("silver_sales_invoicelines")

df_silver = spark.read.table("silver_sales_invoicelines")

# Get the list of columns from the DataFrame
columns = df_silver.columns

# Loop through each column and apply type casting rules based on naming conventions
for col_name in columns:
    # 1. Cast Identifiers to Integer (e.g., CustomerID, InvoiceLineID)
    if col_name.endswith("ID"):
        df_silver = df_silver.withColumn(col_name, col(col_name).cast("int"))

    # 2. Cast Dates (e.g., OrderDate, InvoiceDate)
    elif "Date" in col_name:
        df_silver = df_silver.withColumn(col_name, col(col_name).cast("date"))

    # 3. Cast Financials to Decimal (e.g., UnitPrice, TaxAmount, GrossProfit)
    elif any(keyword in col_name for keyword in ["Price", "Tax", "Profit", "Amount"]):
        df_silver = df_silver.withColumn(col_name, col(col_name).cast("decimal(18,2)"))

    # 4. Cast Quantities to Integer (e.g., Quantity, OrderQuantity)
    elif "Quantity" in col_name:
        df_silver = df_silver.withColumn(col_name, col(col_name).cast("int"))

# Optionally, persist the updated schema back to the same Silver table (overwrite mode)
# Be careful: this will replace the existing table definition and data
# Uncomment the following line if you want to save the changes back to the Lakehouse table

# df_silver.write.mode("overwrite").saveAsTable("silver_sales_invoicelines")

# Display a sample of the transformed data and the new schema for validation
print("=== UPDATED SCHEMA ===")
df_silver.printSchema()

print("\n=== SAMPLE DATA (AFTER CASTING) ===")
display(df_silver.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json

print("🚀 Scanning default Silver Lakehouse for all table schemas...")

# 1. Use raw SQL to get tables in the current default context (Silver)
tables_df = spark.sql("SHOW TABLES")
table_names = [row.tableName for row in tables_df.collect()]

master_dictionary = {}

for table_name in table_names:
    # 2. Read the table metadata directly
    df_silver = spark.table(table_name)
    
    # 3. Extract columns and their current Spark data types
    schema_dict = {col_name: dtype for col_name, dtype in df_silver.dtypes}
    
    master_dictionary[table_name] = schema_dict

print("\n✅ Extraction Complete. Here is your baseline Metadata Dictionary:\n")

# 4. Print as a cleanly formatted JSON string
print(json.dumps(master_dictionary, indent=4))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col

# 1. The Comprehensive Data Contract for WWI Operational Tables
schema_contract = {
    # --- APPLICATION SCHEMA ---
    "silver_application_cities": {
        "CityID": "int", "StateProvinceID": "int", "LatestRecordedPopulation": "bigint", "ValidFrom": "timestamp", "ValidTo": "timestamp"
    },
    "silver_application_countries": {
        "CountryID": "int", "LatestRecordedPopulation": "bigint", "ValidFrom": "timestamp", "ValidTo": "timestamp"
    },
    "silver_application_people": {
        "PersonID": "int", "IsPermittedToLogon": "int", "IsExternalLogonProvider": "int", "IsSystemUser": "int", "IsEmployee": "int", "IsSalesperson": "int", "ValidFrom": "timestamp", "ValidTo": "timestamp"
    },
    "silver_application_stateprovinces": {
        "StateProvinceID": "int", "CountryID": "int", "LatestRecordedPopulation": "bigint", "ValidFrom": "timestamp", "ValidTo": "timestamp"
    },
    
    # --- PURCHASING SCHEMA ---
    "silver_purchasing_purchaseorderlines": {
        "PurchaseOrderLineID": "int", "PurchaseOrderID": "int", "StockItemID": "int", "OrderedOuters": "int", "ReceivedOuters": "int", "PackageTypeID": "int", "ExpectedUnitPricePerOuter": "decimal(18,2)", "LastReceiptDate": "date", "IsOrderLineFinalized": "int", "LastEditedWhen": "timestamp"
    },
    "silver_purchasing_purchaseorders": {
        "PurchaseOrderID": "int", "SupplierID": "int", "OrderDate": "date", "DeliveryMethodID": "int", "ContactPersonID": "int", "ExpectedDeliveryDate": "date", "IsOrderFinalized": "int", "LastEditedWhen": "timestamp"
    },
    "silver_purchasing_suppliers": {
        "SupplierID": "int", "SupplierCategoryID": "int", "PrimaryContactPersonID": "int", "AlternateContactPersonID": "int", "DeliveryMethodID": "int", "DeliveryCityID": "int", "PostalCityID": "int", "PaymentDays": "int", "ValidFrom": "timestamp", "ValidTo": "timestamp"
    },
    "silver_purchasing_suppliertransactions": {
        "SupplierTransactionID": "int", "SupplierID": "int", "TransactionTypeID": "int", "PurchaseOrderID": "int", "PaymentMethodID": "int", "TransactionDate": "date", "AmountExcludingTax": "decimal(18,2)", "TaxAmount": "decimal(18,2)", "TransactionAmount": "decimal(18,2)", "OutstandingBalance": "decimal(18,2)", "FinalizationDate": "date", "IsFinalized": "int", "LastEditedWhen": "timestamp"
    },

    # --- SALES SCHEMA ---
    "silver_sales_customers": {
        "CustomerID": "int", "BillToCustomerID": "int", "CustomerCategoryID": "int", "BuyingGroupID": "int", "PrimaryContactPersonID": "int", "AlternateContactPersonID": "int", "DeliveryMethodID": "int", "DeliveryCityID": "int", "PostalCityID": "int", "CreditLimit": "decimal(18,2)", "AccountOpenedDate": "date", "StandardDiscountPercentage": "decimal(18,3)", "IsStatementSent": "int", "IsOnCreditHold": "int", "PaymentDays": "int", "ValidFrom": "timestamp", "ValidTo": "timestamp"
    },
    "silver_sales_customertransactions": {
        "CustomerTransactionID": "int", "CustomerID": "int", "TransactionTypeID": "int", "InvoiceID": "int", "PaymentMethodID": "int", "TransactionDate": "date", "AmountExcludingTax": "decimal(18,2)", "TaxAmount": "decimal(18,2)", "TransactionAmount": "decimal(18,2)", "OutstandingBalance": "decimal(18,2)", "FinalizationDate": "date", "IsFinalized": "int", "LastEditedWhen": "timestamp"
    },
    "silver_sales_invoicelines": {
        "InvoiceLineID": "int", "InvoiceID": "int", "StockItemID": "int", "PackageTypeID": "int", "Quantity": "int", "UnitPrice": "decimal(18,2)", "TaxRate": "decimal(18,3)", "TaxAmount": "decimal(18,2)", "LineProfit": "decimal(18,2)", "ExtendedPrice": "decimal(18,2)", "LastEditedWhen": "timestamp"
    },
    "silver_sales_invoices": {
        "InvoiceID": "int", "CustomerID": "int", "BillToCustomerID": "int", "OrderID": "int", "DeliveryMethodID": "int", "ContactPersonID": "int", "AccountsPersonID": "int", "SalespersonPersonID": "int", "PackedByPersonID": "int", "InvoiceDate": "date", "IsCreditNote": "int", "TotalDryItems": "int", "TotalChillerItems": "int", "ConfirmedDeliveryTime": "timestamp", "LastEditedWhen": "timestamp"
    },
    "silver_sales_orderlines": {
        "OrderLineID": "int", "OrderID": "int", "StockItemID": "int", "PackageTypeID": "int", "Quantity": "int", "UnitPrice": "decimal(18,2)", "TaxRate": "decimal(18,3)", "PickedQuantity": "int", "PickingCompletedWhen": "timestamp", "LastEditedWhen": "timestamp"
    },
    "silver_sales_orders": {
        "OrderID": "int", "CustomerID": "int", "SalespersonPersonID": "int", "PickedByPersonID": "int", "ContactPersonID": "int", "BackorderOrderID": "int", "OrderDate": "date", "ExpectedDeliveryDate": "date", "IsUndersupplyBackordered": "int", "PickingCompletedWhen": "timestamp", "LastEditedWhen": "timestamp"
    },

    # --- WAREHOUSE SCHEMA ---
    "silver_warehouse_coldroomtemperatures": {
        "ColdRoomTemperatureID": "int", "ColdRoomSensorNumber": "int", "RecordedWhen": "timestamp", "Temperature": "decimal(18,2)", "ValidFrom": "timestamp", "ValidTo": "timestamp"
    },
    "silver_warehouse_stockitemholdings": {
        "StockItemID": "int", "QuantityOnHand": "int", "LastStocktakeQuantity": "int", "LastCostPrice": "decimal(18,2)", "ReorderLevel": "int", "TargetStockLevel": "int", "LastEditedWhen": "timestamp"
    },
    "silver_warehouse_stockitems": {
        "StockItemID": "int", "SupplierID": "int", "ColorID": "int", "UnitPackageID": "int", "OuterPackageID": "int", "LeadTimeDays": "int", "QuantityPerOuter": "int", "IsChillerStock": "int", "TaxRate": "decimal(18,3)", "UnitPrice": "decimal(18,2)", "RecommendedRetailPrice": "decimal(18,2)", "TypicalWeightPerUnit": "decimal(18,3)", "ValidFrom": "timestamp", "ValidTo": "timestamp"
    },
    "silver_warehouse_stockitemtransactions": {
        "StockItemTransactionID": "int", "StockItemID": "int", "TransactionTypeID": "int", "CustomerID": "int", "InvoiceID": "int", "SupplierID": "int", "PurchaseOrderID": "int", "TransactionOccurredWhen": "timestamp", "Quantity": "decimal(18,3)", "LastEditedWhen": "timestamp"
    },
    "silver_warehouse_vehicletemperatures": {
        "VehicleTemperatureID": "int", "ChillerSensorNumber": "int", "RecordedWhen": "timestamp", "Temperature": "decimal(18,2)", "IsCompressed": "int"
    }
}

print("🚀 Enforcing the Comprehensive Data Contract...")

# 2. Apply the specific casts defined in the contract
for table_name, column_rules in schema_contract.items():
    print(f"\n⚙️ Processing {table_name}...")
    
    try:
        df_silver = spark.read.table(table_name)
        existing_columns = df_silver.columns
        
        for col_name, data_type in column_rules.items():
            if col_name in existing_columns:
                df_silver = df_silver.withColumn(col_name, col(col_name).cast(data_type))
        
        # Overwrite the table with the strictly typed schema
        df_silver.write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(table_name)
            
        print(f"✅ Schema enforced for {table_name}.")
        
    except Exception as e:
        print(f"❌ Failed to process {table_name}. Error: {e}")

print("\n🏁 Comprehensive Silver Layer foundation is strictly typed. PySpark engineering is complete.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, trim, when
from pyspark.sql.types import StringType

# 1. Target the operational tables we just strictly typed
target_tables = [
    "silver_application_cities", "silver_application_countries", "silver_application_people", "silver_application_stateprovinces",
    "silver_purchasing_purchaseorderlines", "silver_purchasing_purchaseorders", "silver_purchasing_suppliers", "silver_purchasing_suppliertransactions",
    "silver_sales_customers", "silver_sales_customertransactions", "silver_sales_invoicelines", "silver_sales_invoices", "silver_sales_orderlines", "silver_sales_orders",
    "silver_warehouse_coldroomtemperatures", "silver_warehouse_stockitemholdings", "silver_warehouse_stockitems", "silver_warehouse_stockitemtransactions", "silver_warehouse_vehicletemperatures"
]

print("🚀 Initiating Bulk Data Standardization (Whitespace & NULLs)...")

for table_name in target_tables:
    try:
        df_silver = spark.read.table(table_name)
        
        # 2. Programmatically identify only the String columns
        string_columns = [f.name for f in df_silver.schema.fields if isinstance(f.dataType, StringType)]
        
        if not string_columns:
            print(f"⏩ {table_name}: No string columns found. Skipping.")
            continue
            
        print(f"⚙️ Processing {table_name} (Standardizing {len(string_columns)} text columns)...")
        
        # 3. Apply standardizations dynamically
        for c in string_columns:
            # Trim whitespace and replace empty strings with actual NULLs
            df_silver = df_silver.withColumn(
                c, 
                when(trim(col(c)) == "", None).otherwise(trim(col(c)))
            )
            
        # 4. Overwrite the table
        df_silver.write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(table_name)
            
        print(f"✅ Cleaned and saved: {table_name}")
        
    except Exception as e:
        print(f"❌ Failed to standardize {table_name}. Error: {e}")

print("\n🏁 Universal standardization complete. The Silver Layer is bulletproof.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, sha2, when

print("🚀 Initiating Business Validation and PII Masking...")

# ==========================================
# STEP 1: VALIDATION & FILTERING
# ==========================================
print("\n⚙️ Validating Fact Tables (Dropping logically impossible records)...")

# 1. Invoice Lines: Quantity and Unit Price can never be negative in this system.
df_invoicelines = spark.read.table("silver_sales_invoicelines")
original_count = df_invoicelines.count()

df_invoicelines = df_invoicelines.filter((col("Quantity") >= 0) & (col("UnitPrice") >= 0))
new_count = df_invoicelines.count()

df_invoicelines.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver_sales_invoicelines")
print(f"   -> Validated silver_sales_invoicelines: Dropped {original_count - new_count} invalid rows.")

# ==========================================
# STEP 2: PII MASKING (SHA-256 Hashing)
# ==========================================
print("\n⚙️ Masking PII in Dimension Tables...")

# Define exactly where the PII lives
pii_contract = {
    "silver_application_people": ["PhoneNumber", "FaxNumber", "EmailAddress", "LogonName"],
    "silver_sales_customers": ["PhoneNumber", "FaxNumber"],
    "silver_purchasing_suppliers": ["PhoneNumber", "FaxNumber", "BankAccountNumber"]
}

for table_name, pii_columns in pii_contract.items():
    try:
        df_entity = spark.read.table(table_name)
        existing_columns = df_entity.columns
        
        # Hash the specific columns if they exist
        for c in pii_columns:
            if c in existing_columns:
                # We use sha2(col, 256) to perform a one-way cryptographic hash
                # We also handle NULLs so we don't accidentally hash an empty value into a string
                df_entity = df_entity.withColumn(
                    c, 
                    when(col(c).isNotNull(), sha2(col(c).cast("string"), 256)).otherwise(col(c))
                )
                print(f"   -> Masked {c} in {table_name}")
                
        # Overwrite the table
        df_entity.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)
        print(f"✅ PII secured for {table_name}.")
        
    except Exception as e:
        print(f"❌ Failed to mask {table_name}. Error: {e}")

print("\n🏁 Silver Layer is now fully Validated and Compliant. Close this notebook.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
