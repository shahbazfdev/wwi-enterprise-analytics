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
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Define the relative path to your Bronze folder
folder_path = "Files/"

# mssparkutils interacts directly with OneLake storage
file_list = mssparkutils.fs.ls(folder_path)

# Iterate through the FileInfo objects and print the names
print("Files found in Bronze layer:\n" + "-"*30)
for file in file_list:
    # We only want the files, not subdirectories
    if file.isFile:
        print(file.name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
