# Databricks notebook source
# MAGIC %md
# MAGIC # Note Introduction
# MAGIC ## UI Introduction
# MAGIC ## Magic Commands
# MAGIC - %python
# MAGIC - %sql
# MAGIC - %scala
# MAGIC - %md

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

dbutils.fs.unmount('/mnt/mountPoint')

# COMMAND ----------

# Create MountPoint, pointing to the Container of DataLake (ADLS Gen2) 
configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": "79ff2aaf-f439-4549-8e3b-e8c546a740e1",
          "fs.azure.account.oauth2.client.secret": "s4y8Q~tfAokiPot4ZMwoooZvGmriKU_peyiGxaB5",
          "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/5c75fce6-1a56-4934-9cf9-7ba0936524aa/oauth2/token"}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://container-learntogether@adlsgen2learntogether.dfs.core.windows.net/",
  mount_point = "/mnt/mountPoint",
  extra_configs = configs)

# COMMAND ----------

dbutils.fs.ls('mnt/mountAdlsGen2_Container_LearnTogether')

# COMMAND ----------

# MAGIC %md
# MAGIC Spark Read API

# COMMAND ----------

dataFrame = spark.read.format('csv').options(header=True, inferSchema=True).load('dbfs:/mnt/mountAdlsGen2_Container_LearnTogether/Dir1/circuits.csv')
dataFrame.printSchema()
dataFrame.show(10)
dataFrame.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Spark Write API

# COMMAND ----------

dataFrame.write.format('csv').save('/mnt/mountAdlsGen2_Container_LearnTogether/output/circuits')

# COMMAND ----------

# MAGIC %md
# MAGIC File & Directory Management

# COMMAND ----------

# To remove any directory
# dbutils.fs.rm('dbfs:/mnt/mountAdlsGen2_Container_LearnTogether/test/')
dbutils.fs.rm('dbfs:/mnt/mountAdlsGen2_Container_LearnTogether/test/',recurse=True)

# COMMAND ----------

# To copy file
dbutils.fs.cp('dbfs:/mnt/mountAdlsGen2_Container_LearnTogether/input/circuits.csv','dbfs:/mnt/mountAdlsGen2_Container_LearnTogether/output/')

# COMMAND ----------

# To Move files
dbutils.fs.mv('dbfs:/mnt/mountAdlsGen2_Container_LearnTogether/input/circuits.csv','dbfs:/mnt/mountAdlsGen2_Container_LearnTogether/output/')

# COMMAND ----------

# MAGIC %fs
# MAGIC ls

# COMMAND ----------

# MAGIC %sh
# MAGIC # To list all the processes running in the databricks
# MAGIC ps
