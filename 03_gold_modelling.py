# Databricks notebook source
# MAGIC %sql
# MAGIC USE retail_dw;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dim_customers;
# MAGIC
# MAGIC CREATE TABLE dim_customers AS
# MAGIC SELECT
# MAGIC     ROW_NUMBER() OVER (ORDER BY CustomerID, LastUpdated) AS CustomerSK,
# MAGIC     CustomerID,
# MAGIC     CustomerName,
# MAGIC     Email,
# MAGIC     City,
# MAGIC     Address,
# MAGIC     CURRENT_DATE() AS StartDate,
# MAGIC     CAST('9999-12-31' AS DATE) AS EndDate,
# MAGIC     1 AS IsActive
# MAGIC FROM silver_customers;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dim_products;
# MAGIC
# MAGIC CREATE TABLE dim_products AS
# MAGIC
# MAGIC SELECT
# MAGIC     ROW_NUMBER() OVER (ORDER BY ProductID) AS ProductSK,
# MAGIC     ProductID,
# MAGIC     ProductName,
# MAGIC     Category,
# MAGIC     UnitPrice,
# MAGIC     CURRENT_DATE() AS EffectiveDate
# MAGIC
# MAGIC FROM silver_products;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dim_stores;
# MAGIC
# MAGIC CREATE TABLE dim_stores AS
# MAGIC
# MAGIC SELECT
# MAGIC     ROW_NUMBER() OVER (ORDER BY StoreID) AS StoreSK,
# MAGIC     StoreID,
# MAGIC     StoreName,
# MAGIC     Region
# MAGIC
# MAGIC FROM silver_stores;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS fact_sales;
# MAGIC
# MAGIC CREATE TABLE fact_sales AS
# MAGIC
# MAGIC SELECT
# MAGIC     ROW_NUMBER() OVER (ORDER BY s.TransactionID) AS SalesSK,
# MAGIC     s.TransactionID,
# MAGIC     c.CustomerSK,
# MAGIC     p.ProductSK,
# MAGIC     st.StoreSK,
# MAGIC     s.Quantity,
# MAGIC     s.Quantity * p.UnitPrice AS Amount,
# MAGIC     s.TxnDate
# MAGIC
# MAGIC FROM silver_sales s
# MAGIC
# MAGIC JOIN (
# MAGIC     SELECT *
# MAGIC     FROM (
# MAGIC         SELECT *,
# MAGIC                ROW_NUMBER() OVER (
# MAGIC                    PARTITION BY CustomerID
# MAGIC                    ORDER BY CustomerSK DESC
# MAGIC                ) AS rn
# MAGIC         FROM dim_customers
# MAGIC         WHERE IsActive = 1
# MAGIC     )
# MAGIC     WHERE rn = 1
# MAGIC ) c
# MAGIC ON s.CustomerID = c.CustomerID
# MAGIC
# MAGIC JOIN dim_products p
# MAGIC     ON s.ProductID = p.ProductID
# MAGIC
# MAGIC JOIN dim_stores st
# MAGIC     ON s.StoreID = st.StoreID;

# COMMAND ----------

# MAGIC %md
# MAGIC VALIDATION

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dim_customers LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dim_products LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dim_stores LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM fact_sales LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 1. Check row counts
# MAGIC SELECT COUNT(*) FROM dim_customers;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM dim_products;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM dim_stores;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM fact_sales;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT CustomerID, COUNT(*) AS active_count
# MAGIC FROM dim_customers
# MAGIC WHERE IsActive = 1
# MAGIC GROUP BY CustomerID
# MAGIC HAVING COUNT(*) > 1;

# COMMAND ----------

# MAGIC %md
# MAGIC Duplicate CustomerID records were retained with unique surrogate keys for SCD/history analysis and reported as a data quality issue.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dim_customers;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM fact_sales LIMIT 20;