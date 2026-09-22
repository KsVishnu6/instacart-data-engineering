# Databricks notebook source
# Check whether Bronze already contains transaction data

bronze_count = spark.sql("""
    SELECT COUNT(*) AS row_count
    FROM instacart.bronze.orders
""").first()["row_count"]

print(f"Bronze orders row count: {bronze_count}")

# Return the count to ADF
dbutils.notebook.exit(str(bronze_count))