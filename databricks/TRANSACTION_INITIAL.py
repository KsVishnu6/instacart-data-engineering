# Databricks notebook source
# MAGIC %md
# MAGIC WATERMARK CHECK

# COMMAND ----------

from pyspark.sql.functions import max

# Get last processed watermark
watermark_df = spark.sql("""
    SELECT last_watermark
    FROM instacart.control.watermark
    WHERE file_name = 'orders.csv'
""")

last_watermark = watermark_df.first()["last_watermark"]

print(f"Last watermark: {last_watermark}")

# COMMAND ----------

# Read current orders file
orders_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("abfss://raw@instacartstorageadls.dfs.core.windows.net/orders.csv")

# Get current MAX order_id
current_max = orders_df.select(
    max("order_id").alias("current_max")
).first()["current_max"]

print(f"Current MAX order_id: {current_max}")
print(f"Last watermark: {last_watermark}")

# COMMAND ----------

if current_max <= last_watermark:
    print("No new data found. Initial load is not required.")
    dbutils.notebook.exit("NO_NEW_DATA")

print("New data found. Proceeding with initial load.")

# COMMAND ----------

# MAGIC %md
# MAGIC BRONZE LAYER 

# COMMAND ----------

# MAGIC %md
# MAGIC ORDERS TABLE

# COMMAND ----------

orders_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("abfss://raw@instacartstorageadls.dfs.core.windows.net/orders.csv")

orders_bronze_df = orders_df.filter(
    orders_df.eval_set == "prior"
)

orders_bronze_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("instacart.bronze.orders")

# COMMAND ----------

# MAGIC %md
# MAGIC ORDER PRODUCTS PRIOR TABLE
# MAGIC

# COMMAND ----------

order_products_prior_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("abfss://raw@instacartstorageadls.dfs.core.windows.net/order_products__prior.csv")

order_products_prior_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("instacart.bronze.order_products_prior")

# COMMAND ----------

# MAGIC %md
# MAGIC SILVER LAYER

# COMMAND ----------

# MAGIC %md
# MAGIC ORDERS TABLE

# COMMAND ----------

from pyspark.sql.functions import col

# Read Bronze orders
orders_df = spark.table("instacart.bronze.orders")

errors = []


#  CHECK DATA TYPES


expected_types = {
    "order_id": "int",
    "user_id": "int",
    "eval_set": "string",
    "order_number": "int",
    "order_dow": "int",
    "order_hour_of_day": "int",
    "days_since_prior_order": "double"
}

actual_types = dict(orders_df.dtypes)

for column_name, expected_type in expected_types.items():
    actual_type = actual_types.get(column_name)

    if actual_type != expected_type:
        errors.append(
            f"Wrong data type for {column_name}: "
            f"expected {expected_type}, found {actual_type}"
        )



#  CHECK REQUIRED NULLS


required_columns = [
    "order_id",
    "user_id",
    "eval_set",
    "order_number",
    "order_dow",
    "order_hour_of_day"
]

for column_name in required_columns:

    null_count = orders_df.filter(
        col(column_name).isNull()
    ).count()

    if null_count > 0:
        errors.append(
            f"{column_name} contains {null_count} NULL values"
        )






#  CHECK DUPLICATE ORDER_ID


duplicate_order_ids = (
    orders_df
    .groupBy("order_id")
    .count()
    .filter(col("count") > 1)
    .count()
)

if duplicate_order_ids > 0:
    errors.append(
        f"Found {duplicate_order_ids} duplicate order_id values"
    )



#  CHECK ORDER_DOW


invalid_order_dow = orders_df.filter(
    (col("order_dow") < 0) |
    (col("order_dow") > 6)
).count()

if invalid_order_dow > 0:
    errors.append(
        f"Found {invalid_order_dow} invalid order_dow values"
    )


#  CHECK ORDER_HOUR_OF_DAY


invalid_order_hours = orders_df.filter(
    (col("order_hour_of_day") < 0) |
    (col("order_hour_of_day") > 23)
).count()

if invalid_order_hours > 0:
    errors.append(
        f"Found {invalid_order_hours} invalid order_hour_of_day values"
    )



#  CHECK ORDER_NUMBER


invalid_order_numbers = orders_df.filter(
    col("order_number") <= 0
).count()

if invalid_order_numbers > 0:
    errors.append(
        f"Found {invalid_order_numbers} invalid order_number values"
    )



#  CHECK USER_ID


invalid_user_ids = orders_df.filter(
    col("user_id") <= 0
).count()

if invalid_user_ids > 0:
    errors.append(
        f"Found {invalid_user_ids} invalid user_id values"
    )



#  CHECK EVAL_SET


invalid_eval_set = orders_df.filter(
    col("eval_set") != "prior"
).count()

if invalid_eval_set > 0:
    errors.append(
        f"Found {invalid_eval_set} rows where eval_set is not 'prior'"
    )



#  FAIL PIPELINE IF ANY CHECK FAILED


if errors:
    error_message = "\n".join(
        f"- {error}" for error in errors
    )

    raise ValueError(
        f"ORDERS DATA QUALITY CHECK FAILED:\n{error_message}"
    )



# ALL CHECKS PASSED


print("ORDERS DATA QUALITY CHECK PASSED")
print("All Silver validation checks passed successfully.")

# COMMAND ----------

orders_silver_df = orders_df

orders_silver_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("instacart.silver.orders")

# COMMAND ----------

# MAGIC %md
# MAGIC ORDER PRODUCTS PRIOR

# COMMAND ----------

from pyspark.sql.functions import col

# Read Bronze order_products_prior
order_products_df = spark.table(
    "instacart.bronze.order_products_prior"
)

errors = []



#  CHECK DATA TYPES


expected_types = {
    "order_id": "int",
    "product_id": "int",
    "add_to_cart_order": "int",
    "reordered": "int"
}

actual_types = dict(order_products_df.dtypes)

for column_name, expected_type in expected_types.items():

    actual_type = actual_types.get(column_name)

    if actual_type != expected_type:
        errors.append(
            f"Wrong data type for {column_name}: "
            f"expected {expected_type}, found {actual_type}"
        )



#  CHECK REQUIRED NULLS


required_columns = [
    "order_id",
    "product_id",
    "add_to_cart_order",
    "reordered"
]

for column_name in required_columns:

    null_count = order_products_df.filter(
        col(column_name).isNull()
    ).count()

    if null_count > 0:
        errors.append(
            f"{column_name} contains {null_count} NULL values"
        )



#  CHECK DUPLICATES


duplicate_rows = (
    order_products_df
    .groupBy("order_id", "product_id")
    .count()
    .filter(col("count") > 1)
    .count()
)

if duplicate_rows > 0:
    errors.append(
        f"Found {duplicate_rows} duplicate "
        f"order_id + product_id combinations"
    )



#  CHECK REORDERED


invalid_reordered = order_products_df.filter(
    ~col("reordered").isin(0, 1)
).count()

if invalid_reordered > 0:
    errors.append(
        f"Found {invalid_reordered} invalid reordered values"
    )



#  CHECK ADD_TO_CART_ORDER


invalid_cart_order = order_products_df.filter(
    col("add_to_cart_order") <= 0
).count()

if invalid_cart_order > 0:
    errors.append(
        f"Found {invalid_cart_order} invalid add_to_cart_order values"
    )



#  CHECK ORDER_ID


invalid_order_ids = order_products_df.filter(
    col("order_id") <= 0
).count()

if invalid_order_ids > 0:
    errors.append(
        f"Found {invalid_order_ids} invalid order_id values"
    )



#  CHECK PRODUCT_ID


invalid_product_ids = order_products_df.filter(
    col("product_id") <= 0
).count()

if invalid_product_ids > 0:
    errors.append(
        f"Found {invalid_product_ids} invalid product_id values"
    )



#  FAIL PIPELINE IF ANY CHECK FAILED


if errors:

    error_message = "\n".join(
        f"- {error}" for error in errors
    )

    raise ValueError(
        f"ORDER_PRODUCTS DATA QUALITY CHECK FAILED:\n"
        f"{error_message}"
    )



# ALL CHECKS PASSED


print("ORDER_PRODUCTS DATA QUALITY CHECK PASSED")
print("All Silver validation checks passed successfully.")

# COMMAND ----------

order_products_silver_df = order_products_df

order_products_silver_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("instacart.silver.order_products_prior")

# COMMAND ----------

# MAGIC %md
# MAGIC GOLD LAYER
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col

# Read Silver tables

order_products_df = spark.table(
    "instacart.silver.order_products_prior"
)

orders_df = spark.table(
    "instacart.silver.orders"
)

products_df = spark.table(
    "instacart.silver.products"
)

# Build fact table

fact_order_products_df = (
    order_products_df
    .join(
        orders_df,
        on="order_id",
        how="inner"
    )
    .join(
        products_df,
        on="product_id",
        how="inner"
    )
    .select(
        "order_id",
        "product_id",
        "user_id",
        "aisle_id",
        "department_id",
        "order_number",
        "order_dow",
        "order_hour_of_day",
        "days_since_prior_order",
        "add_to_cart_order",
        "reordered"
    )
)

display(fact_order_products_df.limit(10))

# COMMAND ----------

fact_order_products_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("instacart.gold.fact_order_products")

# COMMAND ----------

# MAGIC %md
# MAGIC WATERMARK UPDATE
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import max

# Get the latest successfully processed order_id
new_watermark = spark.table(
    "instacart.bronze.orders"
).select(
    max("order_id").alias("max_order_id")
).first()["max_order_id"]

# Update both transaction file watermarks
spark.sql(f"""
    UPDATE instacart.control.watermark
    SET
        last_watermark = {new_watermark},
        updated_at = current_timestamp()
    WHERE file_name IN (
        'orders.csv',
        'order_products__prior.csv'
    )
""")

print(
    f"Transaction watermarks updated successfully to: "
    f"{new_watermark}"
)