# Databricks notebook source
formuladl_demo_access_key = dbutils.secrets.get(scope = 'formula1-scope', key = 'Azure-DataLake-ADLSGen2-AccessKey')
container_name = dbutils.secrets.get(scope = 'formula1-scope', key = 'NameOfContainerInDataLakeADLSGen2')
datalake_adlsgen2_name = dbutils.secrets.get(scope = 'formula1-scope', key = 'NameOfDataLakeADLSGen2')

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.adlsgen2learntogether.dfs.core.windows.net",
    formuladl_demo_access_key
)

# COMMAND ----------

display(dbutils.fs.ls(f"abfss://{container_name}@{datalake_adlsgen2_name}.dfs.core.windows.net"))

# COMMAND ----------

dataframe = spark.read.format('csv').options(header=True, inferSchema=True).load(f'abfss://{container_name}@{datalake_adlsgen2_name}.dfs.core.windows.net/circuits.csv')
dataframe.display()

# COMMAND ----------


