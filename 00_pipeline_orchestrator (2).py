# Databricks notebook source
import re
from datetime import datetime

bucket = "salessprintt9"

folders = {
    "customers": f"s3://{bucket}/sftp/customers/",
    "products": f"s3://{bucket}/sftp/products/",
    "stores": f"s3://{bucket}/sftp/stores/",
    "sales": f"s3://{bucket}/sftp/sales/"
}

archive_base = f"s3://{bucket}/archive/"

# COMMAND ----------

def extract_timestamp(filename):
    pattern = r'_(\d{14})\.csv$'
    match = re.search(pattern, filename)

    if match:
        return datetime.strptime(match.group(1), "%d%m%Y%H%M%S")

    return None

# COMMAND ----------

def get_latest_file(path):
    files = [f for f in dbutils.fs.ls(path) if f.name.endswith(".csv")]

    valid_files = []

    for f in files:
        ts = extract_timestamp(f.name)
        if ts:
            valid_files.append((f, ts))

    if not valid_files:
        raise Exception(f"No valid timestamp CSV files found in {path}")

    valid_files.sort(key=lambda x: x[1], reverse=True)

    latest_file = valid_files[0][0]
    old_files = [x[0] for x in valid_files[1:]]

    return latest_file, old_files

# COMMAND ----------

latest_paths = {}

for entity, path in folders.items():

    latest_file, old_files = get_latest_file(path)

    latest_paths[entity] = latest_file.path

    print(f"Latest {entity} file: {latest_file.name}")

    for old_file in old_files:
        archive_path = f"{archive_base}{entity}/{old_file.name}"

        print(f"Archiving {old_file.path} -> {archive_path}")

        dbutils.fs.cp(old_file.path, archive_path)
        dbutils.fs.rm(old_file.path)

# COMMAND ----------

dbutils.jobs.taskValues.set(key="customers_path", value=latest_paths["customers"])
dbutils.jobs.taskValues.set(key="products_path", value=latest_paths["products"])
dbutils.jobs.taskValues.set(key="stores_path", value=latest_paths["stores"])
dbutils.jobs.taskValues.set(key="sales_path", value=latest_paths["sales"])

print("Paths passed to next task:")
print(latest_paths)