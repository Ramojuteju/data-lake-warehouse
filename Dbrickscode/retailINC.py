# Databricks notebook source
# Spark configuration value
spark.conf.set(
    "fs.azure.account.key.mywarehouse23.dfs.core.windows.net",
    "SSrpPpMFrdI3kaPMWe5Kk9GbQ6D6t+fSx8t76GHYr/YMkgRkIvWPa9n/eMZ5xluG/cWiZHYZpIH8+ASte/DrQw==")

# COMMAND ----------

# Displaying Data Frame in Tabular Format
display(dbutils.fs.ls("abfss://raw@mywarehouse23.dfs.core.windows.net"))

# COMMAND ----------

# Renaming the file using dbutils.fs.mv
dbutils.fs.mv("abfss://raw@mywarehouse23.dfs.core.windows.net/78d9a3a4-7a07-4e1d-97ff-24bc6d2dd964.txt", "abfss://raw@mywarehouse23.dfs.core.windows.net/http.csv")
# Renaming the file using dbutils.fs.mv
dbutils.fs.mv("abfss://raw@mywarehouse23.dfs.core.windows.net/SalesLT.Product.txt", "abfss://raw@mywarehouse23.dfs.core.windows.net/salesproduct.csv")

# COMMAND ----------

# Displaying Data Frame in Tabular Format
display(dbutils.fs.ls("abfss://raw@mywarehouse23.dfs.core.windows.net"))

# COMMAND ----------

# Reading data from raw container
http_df = spark.read.csv("abfss://raw@mywarehouse23.dfs.core.windows.net/http.csv", header=True, inferSchema="true");
http_df.show()
salesproduct_df = spark.read.csv("abfss://raw@mywarehouse23.dfs.core.windows.net/salesproduct.csv", header=True, inferSchema="true");
salesproduct_df.show()

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, DateType

# Defined schemas for each DataFrame
http_schema = StructType([
    StructField("id", IntegerType(), nullable=False),
    StructField("title", StringType(), nullable=True),
    StructField("price", IntegerType(), nullable=False),
    StructField("description", StringType(), nullable=True),
    StructField("category", StringType(), nullable=True),
])

salesproduct_schema = StructType([
    StructField("ProductID", IntegerType(), nullable=False),
    StructField("Name", StringType(), nullable=True),
    StructField("ProductCategoryID", IntegerType(), nullable=False),

])

# COMMAND ----------

from pyspark.sql import functions as DE
# Dropping columns
http_df = http_df.drop("image")
salesproduct_df = salesproduct_df.drop("ProductNumber","Color","StandardCost","ListPrice","Size","Weight","ProductModelID","SellStartDate","SellEndDate","DiscontinuedDate","ThumbNailPhoto","ThumbnailPhotoFileName","rowguid","ModifiedDate")


# COMMAND ----------

#Loading Data Using Explicit/specified Schemas
http_df = spark.read.csv("abfss://raw@mywarehouse23.dfs.core.windows.net/http.csv", schema=http_schema, header=True)
salesproduct_df = spark.read.csv("abfss://raw@mywarehouse23.dfs.core.windows.net/salesproduct.csv", schema=salesproduct_schema, header=True)

# COMMAND ----------

# Define the paths to the curated container
curated_http = "abfss://meta@casestrge23.dfs.core.windows.net/delta/http_delta"
curated_salesproduct = "abfss://meta@casestrge23.dfs.core.windows.net/delta/salesproduct_delta"

# COMMAND ----------

# Now write the cleaned DataFrames
http_df_cleaned.write.format("delta").mode("append").save("abfss://curated@mywarehouse23.dfs.core.windows.net/http_delta")
salesproduct_df_cleaned.write.format("delta").mode("append").save("abfss://curated@mywarehouse23.dfs.core.windows.net/salesproduct_delta")

# COMMAND ----------

# Displaying Data Frame in Tabular Format
display(dbutils.fs.ls("abfss://curated@mywarehouse23.dfs.core.windows.net"))