-- Databricks notebook source
USE retail_dw;

-- COMMAND ----------

DROP TABLE IF EXISTS silver_customers;

CREATE TABLE silver_customers AS
SELECT DISTINCT
    CAST(CustomerID AS INT) AS CustomerID,
    INITCAP(TRIM(CustomerName)) AS CustomerName,
    LOWER(TRIM(Email)) AS Email,
    TRIM(City) AS City,
    TRIM(Address) AS Address,
    TO_DATE(LastUpdated, 'dd-MM-yyyy') AS LastUpdated
FROM bronze_customers;

-- COMMAND ----------

DROP TABLE IF EXISTS silver_products;

CREATE TABLE silver_products AS
SELECT DISTINCT
    CAST(ProductID AS INT) AS ProductID,
    TRIM(ProductName) AS ProductName,
    TRIM(Category) AS Category,
    CAST(UnitPrice AS DECIMAL(10,2)) AS UnitPrice
FROM bronze_products
WHERE UnitPrice > 0;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC

-- COMMAND ----------

DROP TABLE IF EXISTS silver_stores;

CREATE TABLE silver_stores AS
SELECT DISTINCT
    CAST(StoreID AS INT) AS StoreID,
    INITCAP(TRIM(StoreName)) AS StoreName,
    COALESCE(NULLIF(TRIM(Region), ''), 'Unknown') AS Region
FROM bronze_stores;

-- COMMAND ----------

DROP TABLE IF EXISTS silver_sales;

CREATE TABLE silver_sales AS

SELECT DISTINCT
    CAST(TransactionID AS INT) AS TransactionID,
    CAST(CustomerID AS INT) AS CustomerID,
    CAST(ProductID AS INT) AS ProductID,
    CAST(StoreID AS INT) AS StoreID,
    CAST(Quantity AS INT) AS Quantity,
    TO_DATE(TxnDate, 'dd-MM-yyyy') AS TxnDate

FROM bronze_sales

WHERE CAST(Quantity AS INT) > 0;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC -----------------VALIDATION---------

-- COMMAND ----------

-- MAGIC %md
-- MAGIC VALIDATE CUSTOMER TABLE

-- COMMAND ----------

-- MAGIC %md
-- MAGIC now , lets validate silver stage 

-- COMMAND ----------

SELECT CustomerID, COUNT(*)
FROM silver_customers
GROUP BY CustomerID
HAVING COUNT(*) > 1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC validation query for data integrety issue 

-- COMMAND ----------

SELECT *
FROM silver_customers
WHERE CustomerID IN (485,486,487,488,489)
ORDER BY CustomerID;

-- COMMAND ----------

SELECT *
FROM silver_products
WHERE UnitPrice <= 0;

-- COMMAND ----------

-- 1. Duplicate CustomerID
SELECT CustomerID, COUNT(*) AS duplicate_count
FROM silver_customers
GROUP BY CustomerID
HAVING COUNT(*) > 1;

-- COMMAND ----------

-- 2. Same CustomerID mapped to different names/emails
SELECT 
    CustomerID,
    COUNT(DISTINCT CustomerName) AS name_count,
    COUNT(DISTINCT Email) AS email_count
FROM silver_customers
GROUP BY CustomerID
HAVING COUNT(DISTINCT CustomerName) > 1
   OR COUNT(DISTINCT Email) > 1;

-- COMMAND ----------

-- 3. Null check - Customers
SELECT *
FROM silver_customers
WHERE CustomerID IS NULL
   OR CustomerName IS NULL
   OR Email IS NULL
   OR City IS NULL
   OR Address IS NULL;

-- COMMAND ----------

-- error 
-- 4. Invalid product price
SELECT *
FROM silver_products
WHERE UnitPrice <= 0 OR UnitPrice IS NULL;

-- COMMAND ----------

-- 5. Missing store region
SELECT *
FROM silver_stores
WHERE Region IS NULL OR TRIM(Region) = '';

-- COMMAND ----------

-- 6. Duplicate TransactionID
SELECT TransactionID, COUNT(*) AS duplicate_count
FROM silver_sales
GROUP BY TransactionID
HAVING COUNT(*) > 1;

-- COMMAND ----------

-- 7. Invalid quantity
SELECT *
FROM silver_sales
WHERE Quantity <= 0 OR Quantity IS NULL;

-- COMMAND ----------

--. Sales records with CustomerID not present in customer file
SELECT s.*
FROM silver_sales s
LEFT JOIN silver_customers c
ON s.CustomerID = c.CustomerID
WHERE c.CustomerID IS NULL;

-- COMMAND ----------

SELECT TransactionID, COUNT(*) AS duplicate_count
FROM silver_sales
GROUP BY TransactionID
HAVING COUNT(*) > 1;

-- COMMAND ----------

SELECT *
FROM silver_sales
WHERE TransactionID = 2481;

-- COMMAND ----------

select count(*) from silver_customers;

-- COMMAND ----------

select count(*) from silver_products;

-- COMMAND ----------

select count(*) from silver_sales;

-- COMMAND ----------

select count(*) from silver_stores;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC