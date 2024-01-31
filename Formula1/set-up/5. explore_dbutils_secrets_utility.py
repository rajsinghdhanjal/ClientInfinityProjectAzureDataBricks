# Databricks notebook source
# MAGIC %md
# MAGIC #### Explore the Capabilities of the 'dbutils.secrets' Utility

# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list(scope = 'formula1-scope')

# COMMAND ----------

dbutils.secrets.get(scope = 'formula1-scope', key = 'Azure-DataLake-ADLSGen2-AccessKey')
dbutils.secrets.get(scope = 'formula1-scope', key = 'ClientSecretValueForServicePrincipalAppRegForAccessingADLSGen2')
dbutils.secrets.get(scope = 'formula1-scope', key = 'NameOfContainerInDataLakeADLSGen2')
dbutils.secrets.get(scope = 'formula1-scope', key = 'NameOfDataLakeADLSGen2')
dbutils.secrets.get(scope = 'formula1-scope', key = 'SAS-Token-Value-To-Access-Container-Named-Demo-In-DataLake-adlsgen2learntogether')
dbutils.secrets.get(scope = 'formula1-scope', key = 'ServicePrincipalAppRegForAccessingADLSGen2-ApplicationOrClientID')
dbutils.secrets.get(scope = 'formula1-scope', key = 'ServicePrincipalAppRegForAccessingADLSGen2-DirectoryOrTenantID')

# COMMAND ----------

datalakeAccessKey = dbutils.secrets.get(scope = 'formula1-scope', key = 'Azure-DataLake-ADLSGen2-AccessKey')
print(datalakeAccessKey)

# COMMAND ----------


