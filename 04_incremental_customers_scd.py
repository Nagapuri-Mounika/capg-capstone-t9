# Databricks notebook source
# MAGIC %sql
# MAGIC USE retail_dw;

# COMMAND ----------

# MAGIC %sql
# MAGIC --detect changed customers
# MAGIC CREATE OR REPLACE TEMP VIEW changed_customers AS
# MAGIC
# MAGIC SELECT
# MAGIC     s.CustomerID,
# MAGIC     s.CustomerName,
# MAGIC     s.Email,
# MAGIC     s.City,
# MAGIC     s.Address,
# MAGIC     s.LastUpdated
# MAGIC
# MAGIC FROM silver_customers s
# MAGIC
# MAGIC JOIN dim_customers d
# MAGIC ON s.CustomerID = d.CustomerID
# MAGIC
# MAGIC WHERE d.IsActive = 1
# MAGIC
# MAGIC AND (
# MAGIC     s.City <> d.City
# MAGIC     OR s.Address <> d.Address
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC --expire old records
# MAGIC UPDATE dim_customers
# MAGIC
# MAGIC SET
# MAGIC     EndDate = CURRENT_DATE(),
# MAGIC     IsActive = 0
# MAGIC
# MAGIC WHERE CustomerID IN (
# MAGIC
# MAGIC     SELECT CustomerID
# MAGIC
# MAGIC     FROM changed_customers
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC --insert new + changed rowa
# MAGIC INSERT INTO dim_customers
# MAGIC
# MAGIC SELECT
# MAGIC     (SELECT COALESCE(MAX(CustomerSK), 0)
# MAGIC      FROM dim_customers)
# MAGIC
# MAGIC     + ROW_NUMBER() OVER (ORDER BY s.CustomerID) AS CustomerSK,
# MAGIC
# MAGIC     s.CustomerID,
# MAGIC     s.CustomerName,
# MAGIC     s.Email,
# MAGIC     s.City,
# MAGIC     s.Address,
# MAGIC
# MAGIC     CURRENT_DATE() AS StartDate,
# MAGIC
# MAGIC     CAST('9999-12-31' AS DATE) AS EndDate,
# MAGIC
# MAGIC     1 AS IsActive
# MAGIC
# MAGIC FROM silver_customers s
# MAGIC
# MAGIC LEFT JOIN dim_customers d
# MAGIC ON s.CustomerID = d.CustomerID
# MAGIC AND d.IsActive = 1
# MAGIC
# MAGIC WHERE d.CustomerID IS NULL
# MAGIC
# MAGIC OR s.CustomerID IN (
# MAGIC     SELECT CustomerID
# MAGIC     FROM changed_customers
# MAGIC );