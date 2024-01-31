# Databricks notebook source
formuladl_demo_sas_token_value = dbutils.secrets.get(scope = 'formula1-scope', key = 'SAS-Token-Value-To-Access-Container-Named-Demo-In-DataLake-adlsgen2learntogether')
container_name = dbutils.secrets.get(scope = 'formula1-scope', key = 'NameOfContainerInDataLakeADLSGen2')
datalake_adlsgen2_name = dbutils.secrets.get(scope = 'formula1-scope', key = 'NameOfDataLakeADLSGen2')

# COMMAND ----------

spark.conf.set(f"fs.azure.account.auth.type.{datalake_adlsgen2_name}.dfs.core.windows.net", "SAS")
spark.conf.set(
    f"fs.azure.sas.token.provider.type.{datalake_adlsgen2_name}.dfs.core.windows.net", 
    "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set(
    "fs.azure.sas.fixed.token.adlsgen2learntogether.dfs.core.windows.net", 
    formuladl_demo_sas_token_value
    )

# COMMAND ----------

from pyspark.sql.types import StructType

# spark.read.load(f"abfss://{container_name}@{datalake_adlsgen2_name}.dfs.core.windows.net/circuits.csv")
df = dbutils.fs.ls(f"abfss://{container_name}@{datalake_adlsgen2_name}.dfs.core.windows.net")

display(df)

#if df.isEmpty():
#    print("No Directories or Files found in the Container!")
#else:
#    display(dbutils.fs.ls(f"abfss://{container_name}@{datalake_adlsgen2_name}.dfs.core.windows.net"))


# COMMAND ----------

dataframe = spark.read.format('csv').options(header=True, inferSchema=True).load(f'abfss://{container_name}@{datalake_adlsgen2_name}.dfs.core.windows.net/circuits.csv')
dataframe.display()

# COMMAND ----------


