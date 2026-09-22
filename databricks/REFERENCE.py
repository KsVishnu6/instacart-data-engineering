# Databricks notebook source
# MAGIC %md
# MAGIC BRONZE LAYER

# COMMAND ----------

# MAGIC %md
# MAGIC PRODUCTS TABLE
# MAGIC

# COMMAND ----------

products_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("quote", '"') \
    .option("escape", '"') \
    .csv("abfss://raw@instacartstorageadls.dfs.core.windows.net/products.csv")

products_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("instacart.bronze.products")

# COMMAND ----------

# MAGIC %md
# MAGIC AISLES TABLE

# COMMAND ----------

aisles_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("abfss://raw@instacartstorageadls.dfs.core.windows.net/aisles.csv")

aisles_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("instacart.bronze.aisles")

# COMMAND ----------

# MAGIC %md
# MAGIC DEPARTMENTS TABLE

# COMMAND ----------

departments_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("abfss://raw@instacartstorageadls.dfs.core.windows.net/departments.csv")

departments_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("instacart.bronze.departments")

# COMMAND ----------

# MAGIC %md
# MAGIC SILVER LAYER

# COMMAND ----------

# MAGIC %md
# MAGIC PRODUCTS TABLE

# COMMAND ----------

from pyspark.sql.functions import col

# Read Bronze
products_df = spark.table("instacart.bronze.products")

errors = []


#  CHECK DATA TYPES


expected_types = {
    "product_id": "int",
    "product_name": "string",
    "aisle_id": "int",
    "department_id": "int"
}

actual_types = dict(products_df.dtypes)

for column_name, expected_type in expected_types.items():

    actual_type = actual_types.get(column_name)

    if actual_type != expected_type:
        errors.append(
            f"Wrong data type for {column_name}: "
            f"expected {expected_type}, found {actual_type}"
        )



#  CHECK REQUIRED NULLS


required_columns = [
    "product_id",
    "product_name",
    "aisle_id",
    "department_id"
]

for column_name in required_columns:

    null_count = products_df.filter(
        col(column_name).isNull()
    ).count()

    if null_count > 0:
        errors.append(
            f"{column_name} contains {null_count} NULL values"
        )



#  CHECK DUPLICATE PRODUCT_ID


duplicate_product_ids = (
    products_df
    .groupBy("product_id")
    .count()
    .filter(col("count") > 1)
    .count()
)

if duplicate_product_ids > 0:
    errors.append(
        f"Found {duplicate_product_ids} duplicate product_id values"
    )



#  CHECK PRODUCT_ID


invalid_product_ids = products_df.filter(
    col("product_id") <= 0
).count()

if invalid_product_ids > 0:
    errors.append(
        f"Found {invalid_product_ids} invalid product_id values"
    )



#  CHECK AISLE_ID


invalid_aisle_ids = products_df.filter(
    col("aisle_id") <= 0
).count()

if invalid_aisle_ids > 0:
    errors.append(
        f"Found {invalid_aisle_ids} invalid aisle_id values"
    )



#  CHECK DEPARTMENT_ID


invalid_department_ids = products_df.filter(
    col("department_id") <= 0
).count()

if invalid_department_ids > 0:
    errors.append(
        f"Found {invalid_department_ids} invalid department_id values"
    )



#  CHECK EMPTY PRODUCT NAME


empty_product_names = products_df.filter(
    col("product_name").isin("", " ")
).count()

if empty_product_names > 0:
    errors.append(
        f"Found {empty_product_names} empty product_name values"
    )



# FAIL IF ANY CHECK FAILED


if errors:

    error_message = "\n".join(
        f"- {error}" for error in errors
    )

    raise ValueError(
        f"PRODUCTS DATA QUALITY CHECK FAILED:\n"
        f"{error_message}"
    )



# ALL CHECKS PASSED


print("PRODUCTS DATA QUALITY CHECK PASSED")
print("All Silver validation checks passed successfully.")

# COMMAND ----------

products_silver_df = spark.table("instacart.bronze.products")

products_silver_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("instacart.silver.products")

# COMMAND ----------

# MAGIC %md
# MAGIC AISLES TABLE

# COMMAND ----------

from pyspark.sql.functions import col

# Read Bronze
aisles_df = spark.table("instacart.bronze.aisles")

errors = []


#  CHECK DATA TYPES


expected_types = {
    "aisle_id": "int",
    "aisle": "string"
}

actual_types = dict(aisles_df.dtypes)

for column_name, expected_type in expected_types.items():

    actual_type = actual_types.get(column_name)

    if actual_type != expected_type:
        errors.append(
            f"Wrong data type for {column_name}: "
            f"expected {expected_type}, found {actual_type}"
        )



#  CHECK REQUIRED NULLS


required_columns = [
    "aisle_id",
    "aisle"
]

for column_name in required_columns:

    null_count = aisles_df.filter(
        col(column_name).isNull()
    ).count()

    if null_count > 0:
        errors.append(
            f"{column_name} contains {null_count} NULL values"
        )



#  CHECK DUPLICATE AISLE_ID


duplicate_aisle_ids = (
    aisles_df
    .groupBy("aisle_id")
    .count()
    .filter(col("count") > 1)
    .count()
)

if duplicate_aisle_ids > 0:
    errors.append(
        f"Found {duplicate_aisle_ids} duplicate aisle_id values"
    )



#  CHECK AISLE_ID


invalid_aisle_ids = aisles_df.filter(
    col("aisle_id") <= 0
).count()

if invalid_aisle_ids > 0:
    errors.append(
        f"Found {invalid_aisle_ids} invalid aisle_id values"
    )



#  CHECK EMPTY AISLE NAME


empty_aisle_names = aisles_df.filter(
    col("aisle").isin("", " ")
).count()

if empty_aisle_names > 0:
    errors.append(
        f"Found {empty_aisle_names} empty aisle values"
    )



#  FAIL IF ANY CHECK FAILED


if errors:

    error_message = "\n".join(
        f"- {error}" for error in errors
    )

    raise ValueError(
        f"AISLES DATA QUALITY CHECK FAILED:\n"
        f"{error_message}"
    )



# ALL CHECKS PASSED


print("AISLES DATA QUALITY CHECK PASSED")
print("All Silver validation checks passed successfully.")

# COMMAND ----------

aisles_silver_df = spark.table("instacart.bronze.aisles")

aisles_silver_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("instacart.silver.aisles")

# COMMAND ----------

# MAGIC %md
# MAGIC DEPARTMENTS TABLE

# COMMAND ----------

from pyspark.sql.functions import col

# Read Bronze
departments_df = spark.table("instacart.bronze.departments")

errors = []


# 1. CHECK DATA TYPES


expected_types = {
    "department_id": "int",
    "department": "string"
}

actual_types = dict(departments_df.dtypes)

for column_name, expected_type in expected_types.items():

    actual_type = actual_types.get(column_name)

    if actual_type != expected_type:
        errors.append(
            f"Wrong data type for {column_name}: "
            f"expected {expected_type}, found {actual_type}"
        )


#  CHECK REQUIRED NULLS


required_columns = [
    "department_id",
    "department"
]

for column_name in required_columns:

    null_count = departments_df.filter(
        col(column_name).isNull()
    ).count()

    if null_count > 0:
        errors.append(
            f"{column_name} contains {null_count} NULL values"
        )



#  CHECK DUPLICATE DEPARTMENT_ID


duplicate_department_ids = (
    departments_df
    .groupBy("department_id")
    .count()
    .filter(col("count") > 1)
    .count()
)

if duplicate_department_ids > 0:
    errors.append(
        f"Found {duplicate_department_ids} duplicate "
        f"department_id values"
    )



#  CHECK DEPARTMENT_ID


invalid_department_ids = departments_df.filter(
    col("department_id") <= 0
).count()

if invalid_department_ids > 0:
    errors.append(
        f"Found {invalid_department_ids} invalid department_id values"
    )



#  CHECK EMPTY DEPARTMENT NAME


empty_department_names = departments_df.filter(
    col("department").isin("", " ")
).count()

if empty_department_names > 0:
    errors.append(
        f"Found {empty_department_names} empty department values"
    )



#  FAIL IF ANY CHECK FAILED


if errors:

    error_message = "\n".join(
        f"- {error}" for error in errors
    )

    raise ValueError(
        f"DEPARTMENTS DATA QUALITY CHECK FAILED:\n"
        f"{error_message}"
    )



# ALL CHECKS PASSED


print("DEPARTMENTS DATA QUALITY CHECK PASSED")
print("All Silver validation checks passed successfully.")

# COMMAND ----------

departments_silver_df = spark.table("instacart.bronze.departments")

departments_silver_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("instacart.silver.departments")

# COMMAND ----------

# MAGIC %md
# MAGIC GOLD LAYER

# COMMAND ----------

# MAGIC %md
# MAGIC PRODUCTS TABLE

# COMMAND ----------

dim_product_df = spark.table("instacart.silver.products")

dim_product_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("instacart.gold.dim_product")

# COMMAND ----------

# MAGIC %md
# MAGIC AISLES TABLE
# MAGIC

# COMMAND ----------

dim_aisle_df = spark.table("instacart.silver.aisles")

dim_aisle_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("instacart.gold.dim_aisle")

# COMMAND ----------

# MAGIC %md
# MAGIC DEPARTMENTS TABLE
# MAGIC

# COMMAND ----------

dim_department_df = spark.table("instacart.silver.departments")

dim_department_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("instacart.gold.dim_department")