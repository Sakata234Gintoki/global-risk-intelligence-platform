# 🌍 Global Risk Intelligence Platform

![Status](https://img.shields.io/badge/Status-Month%201%20In%20Progress-yellow)
![Python](https://img.shields.io/badge/Python-3.13-blue)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-18-blue)
![License](https://img.shields.io/badge/License-MIT-green)

A production-grade data engineering platform that automatically collects global health, finance, and climate data, cleans it, stores it in a structured warehouse, and uses ML to detect risk patterns across countries.

---

## 🚀 What This Project Does

- **Extracts** 429,435 rows of real-world data from WHO, World Bank, and NASA
- **Transforms** and cleans data using pandas (null removal, negative filtering, deduplication)
- **Validates** data quality with 10 automated checks before loading
- **Loads** 395,311 rows into a PostgreSQL star schema data warehouse
- **Orchestrates** pipelines via Apache Airflow (Week 2)
- **Scales** batch processing with PySpark on AWS (Week 3-4)

---

## 🏗️ Architecture

```
Raw Data Sources (WHO / World Bank / NASA)
              ↓
        Extract (Python)
              ↓
     Transform + Clean (pandas)
              ↓
   Quality Validation (10 checks)
              ↓
      Load → PostgreSQL
              ↓
      Star Schema Warehouse
      ├── staging_health_events
      ├── dim_location (237 countries)
      ├── dim_date
      └── fact_health_events (38,909 rows)
              ↓
     Airflow DAGs (orchestration)
              ↓
        AWS S3 (data lake)
              ↓
      PySpark (batch processing)
              ↓
       ML Risk Detection Models
```

---

## 📁 Project Structure

```
global-risk-intelligence-platform/
├── etl/
│   ├── ingest_who.py         # WHO health data pipeline
│   └── validate.py           # Data quality validation framework
├── dags/                     # Airflow DAGs (Week 2)
├── spark/                    # PySpark jobs (Week 4)
├── dbt/                      # dbt transformations (Week 3)
├── sql/
│   └── schema.sql            # PostgreSQL star schema DDL
├── requirements.txt
└── README.md
```

---

## ⚙️ Tech Stack

| Tool | Purpose | Status |
|------|---------|--------|
| Python 3.13 | Core language | ✅ Done |
| pandas 2.2.3 | Data transformation | ✅ Done |
| PostgreSQL 18 | Data warehouse | ✅ Done |
| SQLAlchemy 2.0 | Database ORM | ✅ Done |
| Apache Airflow | Pipeline orchestration | 🔄 Week 2 |
| AWS S3 | Data lake storage | 🔄 Week 3 |
| PySpark | Batch processing | 🔄 Week 4 |
| dbt | Data transformation | 🔄 Week 3 |
| Docker | Containerization | 🔄 Week 2 |
| GitHub Actions | CI/CD | 🔄 Week 4 |

---

## 📊 Current Pipeline Results

```
✅ EXTRACT:     429,435 rows downloaded
✅ TRANSFORM:   395,311 rows cleaned
✅ QUALITY:     10/10 checks passed
✅ LOAD:        395,311 rows → PostgreSQL
✅ FACT TABLE:  38,909 rows loaded
✅ COUNTRIES:   237 countries
✅ DATE RANGE:  2020-01-01 to 2024-08-14
✅ TOTAL TIME:  49 seconds
```

---

## 🗺️ Domains

| Domain | Data Source | Status |
|--------|------------|--------|
| 🏥 Healthcare | WHO COVID-19 Data | ✅ Live |
| 💰 Financial Inclusion | World Bank | 🔄 Week 2 |
| 🌍 Climate Risk | NASA | 🔄 Week 3 |

---

## 📅 Progress

| Milestone | Description | Status |
|-----------|-------------|--------|
| Day 1 | PostgreSQL star schema warehouse | ✅ Done |
| Day 2 | Python ETL pipeline + validation framework | ✅ Done |
| Week 2 | Apache Airflow DAGs + Docker | 🔄 In Progress |
| Week 3 | AWS S3 data lake + dbt | ⏳ Upcoming |
| Week 4 | PySpark batch processing | ⏳ Upcoming |
| Month 2 | ML risk detection models | ⏳ Upcoming |
| Month 3 | AWS deployment + CI/CD | ⏳ Upcoming |

---

## 🛠️ Setup & Run

### 1. Clone the repository
```bash
git clone https://github.com/Sakata234Gintoki/global-risk-intelligence-platform
cd global-risk-intelligence-platform
```

### 2. Install dependencies
```bash
pip install -r requirements.txt --only-binary=:all:
```

### 3. Set up PostgreSQL
- Create database in pgAdmin
- Run `sql/schema.sql` in Query Tool

### 4. Run the pipeline
```bash
python etl/ingest_who.py
```

### 5. Run validation
```bash
python etl/validate.py
```

---

## 👤 Author

**Pranoy Das Eranadath**
- MSc Applied Statistics & Data Analytics — Amrita Vishwa Vidyapeetham
- GitHub: [@Sakata234Gintoki](https://github.com/Sakata234Gintoki)
- Target: Data Engineer / MLE @ Amazon India
