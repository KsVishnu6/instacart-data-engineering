## Project Overview

This project implements an end-to-end data engineering pipeline using the Instacart dataset.

The pipeline ingests raw CSV data into Azure Data Lake Storage Gen2 using Azure Data Factory, processes and validates the data using Azure Databricks, and organizes the data into Bronze, Silver, and Gold layers using Delta tables with Unity Catalog for data governance and table management.

The Gold layer uses a dimensional model consisting of a fact table and dimension tables, which are available for analytical SQL queries through Databricks SQL Warehouse.

The project also implements incremental processing using an order ID watermark, data quality checks, Azure Key Vault for credential management, and ADF retry and failure-alert mechanisms.

## Architecture

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

## Technologies Used

- **Azure Data Factory (ADF)** — Data ingestion, orchestration, scheduling, retries, and failure handling
- **Azure Data Lake Storage Gen2 (ADLS Gen2)** — Raw data storage
- **Azure Blob Storage** — Source file storage
- **Azure Databricks** — Data processing and transformation
- **Delta Lake** — Storage format for Bronze, Silver, and Gold tables
- **Unity Catalog** — Data governance and table management
- **Databricks SQL Warehouse** — SQL-based analytical querying of Gold tables
- **Azure Key Vault** — Secure storage of credentials and secrets

## Data Sources

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




