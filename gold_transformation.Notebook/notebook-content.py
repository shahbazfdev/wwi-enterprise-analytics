# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "0cc05ea7-1e34-4c47-8bfe-65a162747640",
# META       "default_lakehouse_name": "WWI_Gold_Lakehouse",
# META       "default_lakehouse_workspace_id": "49c82d5a-db9c-4bc3-9c11-44ff611516ca",
# META       "known_lakehouses": [
# META         {
# META           "id": "0cc05ea7-1e34-4c47-8bfe-65a162747640"
# META         },
# META         {
# META           "id": "8b1fddab-7f1d-4807-b397-3b7b3b70f8a1"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql.functions import col, expr, year, month, dayofmonth, quarter, dayofweek, when

SILVER_CATALOG = "WWI_Silver_Lakehouse"
GOLD_CATALOG = "WWI_Gold_Lakehouse"
SCHEMA = "dbo"

def silver_table(name):
    return f"{SILVER_CATALOG}.{SCHEMA}.{name}"

def gold_table(name):
    return f"{GOLD_CATALOG}.{SCHEMA}.{name}"

# Get the Gold ABFS path to clean up
gold_info = mssparkutils.lakehouse.get(GOLD_CATALOG)
gold_abfs = gold_info.properties['abfsPath']
tables_path = f"{gold_abfs}/Tables/"

# 1. Clean up all existing Gold folders
print("Cleaning up old Gold folders...")
for tbl in ["Dim_Date", "Dim_Customer", "Dim_StockItem", "Dim_Supplier", "Dim_Employee", "Dim_DeliveryMethod", "Fact_Sales", "Fact_PurchaseOrder"]:
    try:
        mssparkutils.fs.rm(f"{tables_path}{tbl}", True)
        print(f"Removed: {tbl}")
    except:
        pass

# 2. Build Dimensions
print("\nBuilding Dimensions...")
orders_df = spark.table(silver_table("Orders"))
min_date = orders_df.agg({"OrderDate": "min"}).collect()[0][0]
max_date = orders_df.agg({"OrderDate": "max"}).collect()[0][0]

date_df = spark.range(0, (max_date - min_date).days + 1).withColumn(
    "DateKey",
    expr(f"date_add(to_date('{min_date}'), cast(id as int))")
).select(
    col("DateKey").alias("DateKey"),
    col("DateKey").alias("FullDate"),
    year("DateKey").alias("Year"),
    month("DateKey").alias("Month"),
    dayofmonth("DateKey").alias("Day"),
    quarter("DateKey").alias("Quarter"),
    dayofweek("DateKey").alias("DayOfWeek"),
    when(dayofweek("DateKey").isin([1, 7]), "Weekend").otherwise("Weekday").alias("IsWeekend")
)
date_df.write.mode("overwrite").saveAsTable(gold_table("Dim_Date"))

customer_df = spark.table(silver_table("Customers"))
city_df = spark.table(silver_table("Cities"))
state_df = spark.table(silver_table("StateProvinces"))
country_df = spark.table(silver_table("Countries"))

customer_joined = customer_df.join(city_df, customer_df.DeliveryCityID == city_df.CityID, "left") \
                             .join(state_df, city_df.StateProvinceID == state_df.StateProvinceID, "left") \
                             .join(country_df, state_df.CountryID == country_df.CountryID, "left") \
                             .select(
                                 customer_df.CustomerID,
                                 customer_df.CustomerName,
                                 city_df.CityName,
                                 state_df.StateProvinceName,
                                 country_df.CountryName
                             )
customer_joined.distinct().write.mode("overwrite").saveAsTable(gold_table("Dim_Customer"))

stock_df = spark.table(silver_table("StockItems"))
stock_df.select("StockItemID", "StockItemName", "Brand", "Size",
                col("TypicalWeightPerUnit").alias("Weight"),
                "UnitPrice", "RecommendedRetailPrice") \
        .distinct() \
        .write.mode("overwrite").saveAsTable(gold_table("Dim_StockItem"))

spark.table(silver_table("Suppliers")).select("SupplierID", "SupplierName", "SupplierCategoryID") \
     .distinct().write.mode("overwrite").saveAsTable(gold_table("Dim_Supplier"))

spark.table(silver_table("People")).select("PersonID", "FullName", "IsSalesperson") \
     .distinct().write.mode("overwrite").saveAsTable(gold_table("Dim_Employee"))

spark.table(silver_table("DeliveryMethods")).select("DeliveryMethodID", "DeliveryMethodName") \
     .distinct().write.mode("overwrite").saveAsTable(gold_table("Dim_DeliveryMethod"))

print("Dimensions built.")

# 3. Build Facts
print("\nBuilding Facts...")
orderlines_df = spark.table(silver_table("OrderLines"))
orders_df = spark.table(silver_table("Orders"))
stock_df = spark.table(silver_table("StockItems"))
holdings_df = spark.table(silver_table("StockItemHoldings"))

fact_sales = orderlines_df.join(orders_df, orderlines_df.OrderID == orders_df.OrderID, "inner") \
                          .join(stock_df, orderlines_df.StockItemID == stock_df.StockItemID, "left") \
                          .join(holdings_df, stock_df.StockItemID == holdings_df.StockItemID, "left") \
                          .select(
                              orderlines_df.OrderID,
                              orders_df.CustomerID,
                              orders_df.SalespersonPersonID.alias("SalespersonID"),
                              orders_df.OrderDate,
                              orderlines_df.StockItemID,
                              orderlines_df.Quantity,
                              orderlines_df.UnitPrice,
                              holdings_df.LastCostPrice.alias("StandardCost"),
                              ((orderlines_df.UnitPrice - holdings_df.LastCostPrice) * orderlines_df.Quantity).alias("GrossProfit"),
                              ((orderlines_df.UnitPrice - holdings_df.LastCostPrice) / orderlines_df.UnitPrice).alias("ProfitMargin")
                          )
fact_sales = fact_sales.withColumn("DateKey", expr("date_format(OrderDate, 'yyyyMMdd')").cast("int"))
fact_sales.write.mode("overwrite").saveAsTable(gold_table("Fact_Sales"))

pol_df = spark.table(silver_table("PurchaseOrderLines"))
po_df = spark.table(silver_table("PurchaseOrders"))
stock_df = spark.table(silver_table("StockItems"))

fact_purchase = pol_df.join(po_df, pol_df.PurchaseOrderID == po_df.PurchaseOrderID, "inner") \
                      .join(stock_df, pol_df.StockItemID == stock_df.StockItemID, "left") \
                      .select(
                          pol_df.PurchaseOrderID,
                          po_df.SupplierID,
                          pol_df.StockItemID,
                          pol_df.OrderedOuters.alias("OrderedQuantity"),
                          pol_df.ReceivedOuters.alias("ReceivedQuantity"),
                          (pol_df.ReceivedOuters / pol_df.OrderedOuters).alias("FulfillmentRate"),
                          (stock_df.TypicalWeightPerUnit * pol_df.OrderedOuters).alias("TotalWeightKG"),
                          po_df.OrderDate,
                          po_df.ExpectedDeliveryDate,
                          pol_df.LastReceiptDate.alias("ReceivedDate")
                      )
fact_purchase = fact_purchase.withColumn("OrderDateKey", expr("date_format(OrderDate, 'yyyyMMdd')").cast("int"))
fact_purchase = fact_purchase.withColumn("ExpectedDateKey", expr("date_format(ExpectedDeliveryDate, 'yyyyMMdd')").cast("int"))
fact_purchase = fact_purchase.withColumn("ReceivedDateKey", expr("date_format(ReceivedDate, 'yyyyMMdd')").cast("int"))
fact_purchase.write.mode("overwrite").saveAsTable(gold_table("Fact_PurchaseOrder"))

print("Facts built.")

# 4. Verify data immediately in Spark
print("\nVerifying row counts...")
row_count_sales = spark.table(gold_table("Fact_Sales")).count()
row_count_purch = spark.table(gold_table("Fact_PurchaseOrder")).count()
print(f"Fact_Sales: {row_count_sales} rows")
print(f"Fact_PurchaseOrder: {row_count_purch} rows")

# 5. Force SQL endpoint sync
print("\nRefreshing SQL endpoint metadata...")
for tbl in ["Dim_Date", "Dim_Customer", "Dim_StockItem", "Dim_Supplier", "Dim_Employee", "Dim_DeliveryMethod", "Fact_Sales", "Fact_PurchaseOrder"]:
    spark.sql(f"REFRESH TABLE {GOLD_CATALOG}.{SCHEMA}.{tbl}")
    print(f"Refreshed: {tbl}")

print("\nGold rebuild complete. Data is physically written to OneLake.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
