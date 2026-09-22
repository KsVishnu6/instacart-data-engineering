## Project Overview

This project implements an end-to-end data engineering pipeline using the Instacart dataset.

The pipeline ingests raw CSV data into Azure Data Lake Storage Gen2 using Azure Data Factory, processes and validates the data using Azure Databricks, and organizes the data into Bronze, Silver, and Gold layers using Delta tables with Unity Catalog for data governance and table management.

The Gold layer uses a dimensional model consisting of a fact table and dimension tables, which are available for analytical SQL queries through Databricks SQL Warehouse.

The project also implements incremental processing using an order ID watermark, data quality checks, Azure Key Vault for credential management, and ADF retry and failure-alert mechanisms.

## Architecture

```text
Source CSV Files
      ↓
Azure Blob Storage
      ↓
Azure Data Factory
      ↓
Azure Data Lake Storage Gen2
      ↓
Azure Databricks
      ↓
Bronze Layer
      ↓
Silver Layer
(Data Quality & Validation)
      ↓
Gold Layer
(Star Schema)
      ↓
Databricks SQL Warehouse
   
   

**Technologies Used**

- **Azure Data Factory (ADF)** — Data ingestion, orchestration, scheduling, retries, and failure handling
- **Azure Data Lake Storage Gen2 (ADLS Gen2)** — Raw data storage
- **Azure Blob Storage** — Source file storage
- **Azure Databricks** — Data processing and transformation
- **Delta Lake** — Storage format for Bronze, Silver, and Gold tables
- **Unity Catalog** — Data governance and table management
- **Databricks SQL Warehouse** — SQL-based analytical querying of Gold tables
- **Azure Key Vault** — Secure storage of credentials and secrets





**## Data Sources**

The project uses the **Instacart Market Basket Analysis** dataset, which contains customer order history and product information used to analyse purchasing and reordering behaviour.

Source: Kaggle — Instacart Market Basket Analysis

### Source Tables


- **orders.csv** — Contains customer order-level information, including order timing, order sequence, and time since the previous order.  
  **3,421,083 rows, 7 columns**

- **products.csv** — Contains product details and the aisle and department each product belongs to.  
  **49,688 rows, 4 columns**

- **aisles.csv** — Reference table containing aisle names.  
  **134 rows, 2 columns**

- **departments.csv** — Reference table containing department names.  
  **21 rows, 2 columns**

- **order_products__prior.csv** — Contains the products purchased in customers' prior orders, including cart position and whether the product was reordered.  
  **32,434,489 rows, 4 columns**


## ADF Orchestration

Azure Data Factory is used to orchestrate the ingestion and processing workflow.

The pipeline first retrieves the available source files and uses Filter activities to separate them into two paths:

- **Transaction path** — `orders.csv` and `order_products__prior.csv`
- **Reference path** — `products.csv`, `aisles.csv`, and `departments.csv`

For both paths, ForEach activities process the files dynamically and copy them from Azure Blob Storage into ADLS Gen2.

The reference data is processed separately through the reference Databricks notebook.

After the transaction files are copied, the NB_Get_COUNT Databricks notebook checks whether transaction data already exists in the Bronze layer. The result is passed back to ADF and used by an If Condition to determine whether the pipeline should execute the Initial Load or Incremental Load process. Based on this, Databricks processes the data through the Bronze, Silver, and Gold layers, with the Silver layer performing data quality checks and validation, and the Gold layer applying business logic and dimensional data modelling.


## Bronze Layer

The Bronze layer stores the raw source data ingested from Azure Data Lake Storage Gen2 using Azure Databricks.

The data is loaded into Delta tables with minimal transformation so that the original source data is preserved for downstream processing.

The Bronze layer contains:

- `instacart.bronze.orders`
- `instacart.bronze.products`
- `instacart.bronze.aisles`
- `instacart.bronze.departments`
- `instacart.bronze.order_products_prior`



## Silver Layer & Data Quality

The Silver layer contains validated and cleaned data from the Bronze layer.

Before data is written to Silver, data quality checks are performed to identify invalid or inconsistent records.

The checks include:

- Required NULL checks
- Duplicate record checks
- Data type validation
- ID and numeric value validation
- Valid range checks for order day and order hour
- Validation of categorical values such as `eval_set` and `reordered`
- Referential integrity checks for related data

Critical data quality failures stop the pipeline rather than allowing invalid data to continue to the Gold layer.

The validated data is then written to the following Silver tables:

- `instacart.silver.orders`
- `instacart.silver.products`
- `instacart.silver.aisles`
- `instacart.silver.departments`
- `instacart.silver.order_products_prior`


## Gold Layer & Data Modelling

The Gold layer contains business-ready data modelled using a star schema.

The central fact table is maintained at **order-product grain**, where each row represents one product within an order.

### Fact Table

- `instacart.gold.fact_order_products` — Contains order-product level transactional data used for analytical queries.

### Dimension Tables

- `instacart.gold.dim_product` — Contains product details and product hierarchy information.
- `instacart.gold.dim_aisle` — Contains aisle reference information.
- `instacart.gold.dim_department` — Contains department reference information.

The fact table is linked to the dimension tables using product, aisle, and department keys.

## Databricks SQL Warehouse

The Gold tables are exposed through Databricks SQL Warehouse for analytical SQL querying.

The warehouse provides a dedicated SQL serving layer for the Gold dimensional model and can be used by downstream analytics and reporting tools.


## Incremental Processing

The pipeline uses an `order_id` watermark to identify and process newly arrived transaction data.

The current maximum `order_id` is compared with the previously stored watermark. Only records with an `order_id` greater than the stored watermark are processed during an incremental run.

The watermark is updated only after the transaction processing completes successfully.

This prevents previously processed records from being reprocessed on subsequent pipeline runs.



## Error Handling & Monitoring

The pipeline is designed to stop when critical data quality issues are detected rather than allowing invalid data to continue to the Gold layer.

ADF activity retry policies are used to handle temporary failures, and pipeline failure alerts are configured to notify when a pipeline run fails.

The pipeline also uses watermark control to ensure that the watermark is updated only after successful processing.


## Security & Credentials

Azure Key Vault is used to securely store credentials used by Azure Data Factory.

ADF accesses the required secrets through its managed identity rather than storing credentials directly in the pipeline or linked services.

The project uses Unity Catalog for data governance and table management. Databricks accesses ADLS Gen2 through an Azure Access Connector managed identity with Azure RBAC permissions.


## Testing

The pipeline was tested for both initial and incremental processing.

- **Initial load:** Verified that the full historical dataset is loaded into the Bronze, Silver, and Gold layers.
- **Incremental load:** Added new transaction records and verified that only the new records were processed.
- **Watermark validation:** Verified that the watermark is updated to the latest processed `order_id` only after successful processing.
- **Data quality validation:** Verified that critical data quality failures stop the pipeline before invalid data reaches the Gold layer.
- **No-new-data scenario:** Verified that an incremental run stops when no records are newer than the current watermark.

