# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
spark.sql("show schemas in samples").display()

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables in samples.tpch;

# COMMAND ----------

spark.sql("describe extended samples.tpch.customer").display()

# COMMAND ----------

df_customer = spark.read.table("samples.tpch.customer")
display(df_customer)

# COMMAND ----------

from pyspark.sql.functions import *
from functools import reduce
df_customer_nulls = df_customer.where(reduce(lambda a, b: a | b, [col(c).isNull() for c in df_customer.columns]))
df_customer_nulls.display()

# COMMAND ----------

display(df_customer.select("c_custkey").distinct().count())

# COMMAND ----------

df_customer.select(max(col("c_custkey")), min(col("c_custkey"))).display()

# COMMAND ----------

df_customer.select("*").where(col("c_custkey") == 750000).display()

# COMMAND ----------

