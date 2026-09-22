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





