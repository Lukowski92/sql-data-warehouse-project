 📦 SQL Data Warehouse Project

**Built following the course "[SQL Data Warehouse from Scratch](https://www.youtube.com/watch?v=9GVqKuTVANE)" by DataWithBaraa.**

## 🧭 Table of Contents
- [Project Overview](#project-overview)
- [Architecture](#architecture)
- [Repository Structure](#repository-structure)
- [Technologies Used](#technologies-used)
- [ETL & Data Layers](#etl--data-layers)
- [Data Modeling](#data-modeling)
- [Data Quality & Testing](#data-quality--testing)
- [License](#license)

---

## Project Overview
This is a hands-on data engineering project designed to build a complete SQL data warehouse using the Medallion Architecture. The project includes:
- Ingesting CSV data from ERP and CRM systems
- ETL processes: cleansing, transformation, and loading
- Building a star schema for analytics
- Generating business insights and reports

---

## Architecture

Medallion Architecture with three data layers:
1. **Bronze** – Raw ingested data from source files  
2. **Silver** – Cleaned and transformed data  
3. **Gold** – Analytical data models and reporting layer  



---

## Repository Structure
```bash
├── datasets/ # Source CSV files
├── docs/ # Diagrams and documentation
├── scripts/
│ ├── bronze/ # Raw data loading scripts
│ ├── silver/ # Cleansing & transformation scripts
│ └── gold/ # Star schema and final views
├── tests/ # Data quality checks
└── README.md # Project documentation
```

---

## Technologies Used
- SQL Server (or SQL Server Express)
- T-SQL (Transact-SQL) for data processing
- CSV files as data sources
- (Optional) SSMS, Draw.io for diagrams

---
## ETL & Data Layers
-Bronze Layer
Loads raw CSV data into staging tables
Minimal processing, preserves original structure

-Silver Layer
Cleansing (null handling, deduplication)
Formatting and normalization
Prepares data for modeling

-Gold Layer
Star schema: fact and dimension tables
Final views ready for business analysis

## Data Modeling
Star schema with a sales fact table
Dimensions include product, customer, date, and location
Surrogate keys and slowly changing dimensions (SCD) supported
Data model diagrams available in docs/data_models.drawio

## Data Quality & Testing
  The tests/ folder contains scripts for:
-Null and integrity checks
-Duplicate detection
-Referential consistency between dimensions and facts

## License
This project is licensed under the MIT License.
Feel free to use, modify, and distribute it with attribution.

