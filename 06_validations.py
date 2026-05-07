# Databricks notebook source
# MAGIC %sql
# MAGIC use retail_dw;

# COMMAND ----------

# MAGIC %sql
# MAGIC --Duplicate TransactionID
# MAGIC SELECT
# MAGIC     TransactionID,
# MAGIC     COUNT(*) AS cnt
# MAGIC
# MAGIC FROM fact_sales
# MAGIC
# MAGIC GROUP BY TransactionID
# MAGIC
# MAGIC HAVING COUNT(*) > 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC --Active records validation
# MAGIC SELECT *
# MAGIC
# MAGIC FROM dim_customers
# MAGIC
# MAGIC WHERE IsActive = 1
# MAGIC
# MAGIC AND EndDate <> CAST('9999-12-31' AS DATE);

# COMMAND ----------

