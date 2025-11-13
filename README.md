**📘 Project Overview**

An end-to-end Data Engineering pipeline built using the Azure ecosystem to process and analyze Formula 1 race data. The goal was to transform raw, multi-source datasets into clean, structured, and analytics-ready insights using modern data engineering practices.

**⚙️ Process Flow**

**Data Ingestion (ADF):**

a. Used HTTP Web Connectors to fetch multiple datasets (Drivers, Circuits, Constructors, Lap Times).

b. Implemented a parameterized pipeline with lookup configuration for dynamic ingestion.

c. Stored ingested data in Azure Blob Storage as Parquet files.

**Data Processing (Databricks):**

a. Created External Locations and Volumes to access Parquet files.

b. Loaded data into Delta Live Tables (DLT) for schema enforcement and lineage tracking.

c. Applied transformations — removing nulls/duplicates, enforcing constraints, and standardizing data.

**Data Aggregation (Gold Layer):**

a. Built materialized views for key race metrics and performance trends.

b. Connected the final layer to Power BI for interactive visualization and insights.

**🧠 Tech Stack**

Azure Data Factory | Azure Blob Storage | Azure Databricks | Delta Live Tables | PySpark | Power BI

<img width="1110" height="533" alt="image" src="https://github.com/user-attachments/assets/d8c74fe3-a5f4-4d71-af9a-c2358a4791b5" />
