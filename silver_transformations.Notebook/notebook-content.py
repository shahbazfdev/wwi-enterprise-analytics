# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "8b1fddab-7f1d-4807-b397-3b7b3b70f8a1",
# META       "default_lakehouse_name": "WWI_Silver_Lakehouse",
# META       "default_lakehouse_workspace_id": "49c82d5a-db9c-4bc3-9c11-44ff611516ca",
# META       "known_lakehouses": [
# META         {
# META           "id": "8b1fddab-7f1d-4807-b397-3b7b3b70f8a1"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql.functions import col, to_timestamp

# Retrieve the current default lakehouse info
lakehouses = mssparkutils.lakehouse.list()
silver_info = None
for lh in lakehouses:
    if lh.get('isDefault', False):
        silver_info = lh
        break

if silver_info is None:
    # If no default, take the first one (assuming it's the only one)
    silver_info = lakehouses[0]

SILVER_ABFS_PATH = 'abfss://49c82d5a-db9c-4bc3-9c11-44ff611516ca@onelake.dfs.fabric.microsoft.com/8b1fddab-7f1d-4807-b397-3b7b3b70f8a1'
tables_path = f"{SILVER_ABFS_PATH}/Tables/"

master_list = [
    'BuyingGroups', 'Cities', 'Colors', 'Countries', 'CustomerCategories',
    'Customers', 'DeliveryMethods', 'OrderLines', 'Orders', 'PackageTypes',
    'People', 'PurchaseOrderLines', 'PurchaseOrders', 'StateProvinces',
    'StockItems', 'SupplierCategories', 'Suppliers'
]

audit_cols = ['ValidFrom', 'ValidTo', 'LastEditedBy', 'EditedBy', 'RowNumber', 'RowNum']

pk_map = {
    'OrderLines': 'OrderLineID',
    'Orders': 'OrderID',
    'Customers': 'CustomerID',
    'StockItems': 'StockItemID',
    'Suppliers': 'SupplierID',
    'People': 'PersonID',
    'Cities': 'CityID',
    'StateProvinces': 'StateProvinceID',
    'Countries': 'CountryID',
    'BuyingGroups': 'BuyingGroupID',
    'Colors': 'ColorID',
    'PackageTypes': 'PackageTypeID',
    'DeliveryMethods': 'DeliveryMethodID',
    'CustomerCategories': 'CustomerCategoryID',
    'SupplierCategories': 'SupplierCategoryID',
    'PurchaseOrderLines': 'PurchaseOrderLineID',
    'PurchaseOrders': 'PurchaseOrderID'
}

print(f"Using Silver Lakehouse at: {tables_path}")

success_count = 0
failures = []

for table_name in master_list:
    try:
        # Attempt to read from catalog first (if already registered)
        try:
            df = spark.table(table_name)
        except:
            # Fallback to reading from Delta files
            table_path = f"{tables_path}{table_name}"
            df = spark.read.format("delta").load(table_path)
        
        original_count = df.count()

        # Drop audit columns
        existing_cols = df.columns
        drop_cols = [c for c in audit_cols if c in existing_cols]
        if drop_cols:
            df = df.drop(*drop_cols)

        # Drop rows where primary key is null
        pk_col = pk_map.get(table_name)
        if pk_col and pk_col in df.columns:
            df = df.filter(col(pk_col).isNotNull())

        # Cast date columns
        for col_name in df.columns:
            if 'Date' in col_name and df.schema[col_name].dataType.typeName() == 'string':
                df = df.withColumn(col_name, to_timestamp(col(col_name), 'yyyy-MM-dd HH:mm:ss'))

        final_count = df.count()
        # Overwrite the catalog table (register it)
        df.write.mode("overwrite").saveAsTable(table_name)

        print(f"Transformed {table_name}: {original_count} -> {final_count} rows.")
        success_count += 1

    except Exception as e:
        print(f"Error processing {table_name}: {str(e)}")
        failures.append(table_name)

print(f"Completed: {success_count} out of {len(master_list)} tables.")
if failures:
    print(f"Failed: {failures}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
