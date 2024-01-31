# Databricks notebook source
application_or_client_id = dbutils.secrets.get(scope = 'formula1-scope', key = 'ServicePrincipalAppRegForAccessingADLSGen2-ApplicationOrClientID')
directory_or_tenant_id = dbutils.secrets.get(scope = 'formula1-scope', key = 'ServicePrincipalAppRegForAccessingADLSGen2-DirectoryOrTenantID')
client_secret = dbutils.secrets.get(scope = 'formula1-scope', key = 'ClientSecretValueForServicePrincipalAppRegForAccessingADLSGen2')

datalake_adlsgen2_name = dbutils.secrets.get(scope = 'formula1-scope', key = 'NameOfDataLakeADLSGen2')
container_name = dbutils.secrets.get(scope = 'formula1-scope', key = 'NameOfContainerInDataLakeADLSGen2')

# COMMAND ----------

spark.conf.set(f"fs.azure.account.auth.type.{datalake_adlsgen2_name}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{datalake_adlsgen2_name}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{datalake_adlsgen2_name}.dfs.core.windows.net", application_or_client_id)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{datalake_adlsgen2_name}.dfs.core.windows.net", client_secret)
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{datalake_adlsgen2_name}.dfs.core.windows.net", f"https://login.microsoftonline.com/{directory_or_tenant_id}/oauth2/token")

# COMMAND ----------

# dbutils.fs.mount(
#    source = f"abfss://{container_name}@{datalake_adlsgen2_name}.dfs.core.windows.net/",
#    mount_point = "/mnt/mountPoint")

# COMMAND ----------

from pyspark.sql.types import StructType

abfss_path = f"abfss://{container_name}@{datalake_adlsgen2_name}.dfs.core.windows.net"

# spark.read.load(f"abfss://{container_name}@{datalake_adlsgen2_name}.dfs.core.windows.net/circuits.csv")
df = dbutils.fs.ls(abfss_path)

display(df)

# COMMAND ----------

# dataframe = spark.read.format('csv').options(header=True, inferSchema=True).load(f'abfss://{container_name}@{datalake_adlsgen2_name}.dfs.core.windows.net/circuits.csv')

dataframe = spark.read.format('csv').options(header=True, inferSchema=True).load(f'{abfss_path}/circuits.csv')
dataframe.display()

# COMMAND ----------


