# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze

# COMMAND ----------

# MAGIC %md
# MAGIC ## import

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

# MAGIC %md
# MAGIC ## param & widget

# COMMAND ----------

dbutils.widgets.text("city", "lyon")
dbutils.widgets.text("table_bronze", "Listing_detail")
dbutils.widgets.text("file", "listings.csv.gz")
dbutils.widgets.text("path_file", "/Workspace/Users/m.mailleur@gmail.com/fil_rouge/raw/","full path avec / /")

city = dbutils.widgets.get("city")
table_bronze = dbutils.widgets.get("table_bronze")
file = dbutils.widgets.get("file")
path_file = dbutils.widgets.get("path_file")

# COMMAND ----------

# MAGIC %md
# MAGIC ## read

# COMMAND ----------

df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "false")
    .csv(f"{path_file}{city}/{file}")
    .withColumn("city", lit(city))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## write

# COMMAND ----------

df.write.format("delta").mode("overwrite").option("replaceWhere",f"city = '{city}'").saveAsTable(f"bronze.{table_bronze}")