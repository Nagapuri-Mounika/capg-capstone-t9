# Databricks notebook source
customers_path = dbutils.jobs.taskValues.get(
    taskKey="Task1",
    key="customers_path"
)

products_path = dbutils.jobs.taskValues.get(
    taskKey="Task1",
    key="products_path"
)

stores_path = dbutils.jobs.taskValues.get(
    taskKey="Task1",
    key="stores_path"
)

sales_path = dbutils.jobs.taskValues.get(
    taskKey="Task1",
    key="sales_path"
)

print(customers_path)
print(products_path)
print(stores_path)
print(sales_path)

# COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS retail_dw")
spark.sql("USE retail_dw")

# COMMAND ----------

customers_df = spark.read.option("header", "true").csv(customers_path)
products_df = spark.read.option("header", "true").csv(products_path)
stores_df = spark.read.option("header", "true").csv(stores_path)
sales_df = spark.read.option("header", "true").csv(sales_path)

# COMMAND ----------

customers_df.write.mode("overwrite").saveAsTable("bronze_customers")
products_df.write.mode("overwrite").saveAsTable("bronze_products")
stores_df.write.mode("overwrite").saveAsTable("bronze_stores")
sales_df.write.mode("overwrite").saveAsTable("bronze_sales")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronze_customers LIMIT 10;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronze_products LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronze_sales LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronze_stores LIMIT 10;