# Databricks notebook source
# MAGIC %sql
# MAGIC USE retail_dw;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO fact_sales
# MAGIC
# MAGIC SELECT
# MAGIC     (SELECT COALESCE(MAX(SalesSK), 0)
# MAGIC      FROM fact_sales)
# MAGIC     + ROW_NUMBER() OVER (
# MAGIC         ORDER BY s.TransactionID
# MAGIC       ) AS SalesSK,
# MAGIC
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
# MAGIC     ON s.StoreID = st.StoreID
# MAGIC
# MAGIC WHERE s.TransactionID NOT IN (
# MAGIC     SELECT TransactionID
# MAGIC     FROM fact_sales
# MAGIC );