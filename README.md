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




