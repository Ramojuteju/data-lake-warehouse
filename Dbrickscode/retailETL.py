# Databricks notebook source
# Spark configuration value
spark.conf.set(
    "fs.azure.account.key.mywarehouse23.dfs.core.windows.net",
    "SSrpPpMFrdI3kaPMWe5Kk9GbQ6D6t+fSx8t76GHYr/YMkgRkIvWPa9n/eMZ5xluG/cWiZHYZpIH8+ASte/DrQw==")

# COMMAND ----------

# Displaying Data Frame in Tabular Format
display(dbutils.fs.ls("abfss://curated@mywarehouse23.dfs.core.windows.net"))

# COMMAND ----------

# Reading data from silver container
http_df = spark.read.format("delta").load("abfss://curated@mywarehouse23.dfs.core.windows.net/http_delta/")
salesproduct_df = spark.read.format("delta").load("abfss://curated@mywarehouse23.dfs.core.windows.net/salesproduct_delta/")

# COMMAND ----------

# Register the DataFrames as temporary views
http_df.createOrReplaceTempView("http")
salesproduct_df.createOrReplaceTempView("salesproduct")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from http
# MAGIC

# COMMAND ----------

# Rename _sqldf to a new variable name
httpstaging_df = _sqldf

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from salesproduct

# COMMAND ----------

salesstaging_df = _sqldf

# COMMAND ----------

# Define the path for staging container
httpstaging_delta = "abfss://staging@mywarehouse23.dfs.core.windows.net/delta/httpstaging_delta"
salesstaging_delta = "abfss://staging@mywarehouse23.dfs.core.windows.net/delta/salesstaging_delta"

# COMMAND ----------

# Save the DataFrame in Delta format, overwriting if it exists
httpstaging_df.write.format("delta").mode("overwrite").save(httpstaging_delta)
salesstaging_df.write.format("delta").mode("overwrite").save(salesstaging_delta)

# COMMAND ----------

# Displaying Data Frame in Tabular Format
display(dbutils.fs.ls("abfss://staging@mywarehouse23.dfs.core.windows.net/"))