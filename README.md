📦 SQL Data Warehouse Project
Built following the course "SQL Data Warehouse from Scratch" by DataWithBaraa.

🧭 Table of Contents
Project Overview

Architecture

Repository Structure

Technologies Used

How to Run

ETL & Data Layers

Data Modeling

Analysis & Reporting

Data Quality & Testing

License

Project Overview
This is a hands-on data engineering project designed to build a complete SQL data warehouse using the Medallion Architecture. The project includes:

Ingesting CSV data from ERP and CRM systems

ETL processes: cleansing, transformation, and loading

Building a star schema for analytics

Generating business insights and reports

Architecture
Medallion Architecture with three data layers:

Bronze – Raw ingested data from source files

Silver – Cleaned and transformed data

Gold – Analytical data models and reporting layer

For a visual diagram, see files in the docs/ folder.

Repository Structure

bash
.
├── datasets/         # Source CSV files
├── docs/             # Diagrams and documentation
├── scripts/
│   ├── bronze/       # Raw data loading scripts
│   ├── silver/       # Cleansing & transformation scripts
│   └── gold/         # Star schema and final views
├── tests/            # Data quality checks
├── report/           # Reporting & analytical SQL queries
└── README.md         # Project documentation
Technologies Used
SQL Server (or SQL Server Express)

T-SQL (Transact-SQL) for data processing

CSV files as data sources

(Optional) SSMS, Draw.io for diagrams

How to Run
Clone the repository:

git clone https://github.com/Lukowski92/sql-data-warehouse-project.git
cd sql-data-warehouse-project
Ensure your CSV files are placed in the datasets/ directory.

Open the SQL scripts in SSMS and run them in the following order:

sql
-- Example workflow:
USE your_database;

-- Load raw data
:r scripts/bronze/*.sql

-- Clean and normalize
:r scripts/silver/*.sql

-- Build data models
:r scripts/gold/*.sql
Use queries in report/ for data exploration and insights.

ETL & Data Layers
Bronze Layer
Loads raw CSV data into staging tables

Minimal processing, preserves original structure

Silver Layer
Cleansing (null handling, deduplication)

Formatting and normalization

Prepares data for modeling

Gold Layer
Star schema: fact and dimension tables

Final views ready for business analysis

Data Modeling
Star schema with a sales fact table

Dimensions include product, customer, date, and location

Surrogate keys and slowly changing dimensions (SCD) supported

Data model diagrams available in docs/

Data Quality & Testing
The tests/ folder contains scripts for:

Null and integrity checks

Duplicate detection

Referential consistency between dimensions and facts

License
This project is licensed under the MIT License.
Feel free to use, modify, and distribute it with attribution.

