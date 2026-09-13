# 🏥 Real-Time Hospital Patient Flow Analytics Pipeline

![Azure](https://img.shields.io/badge/Azure-Cloud-blue?logo=microsoft-azure&style=flat-square)
![Azure Event Hubs](https://img.shields.io/badge/Azure-Event%20Hubs-blue?logo=microsoft-azure&style=flat-square)
![Databricks](https://img.shields.io/badge/Databricks-PySpark-red?logo=databricks&style=flat-square)
![Azure Synapse](https://img.shields.io/badge/Azure-Synapse%20Analytics-blue?logo=microsoft-azure&style=flat-square)
![Delta Lake](https://img.shields.io/badge/Delta-Lake-blue?logo=databricks&style=flat-square)
![Power BI](https://img.shields.io/badge/Power%20BI-Dashboard-orange?logo=power-bi&style=flat-square)
![Python](https://img.shields.io/badge/Python-3.9+-yellow?logo=python&style=flat-square)
![Git](https://img.shields.io/badge/Git-Version%20Control-green?logo=git&style=flat-square)

---

## 📑 Table of Contents
- [Business Context](#-business-context)
- [Solution Overview](#-solution-overview)
- [Architecture](#-architecture)
- [Project Structure](#-project-structure)
- [Tools & Technologies](#️-tools--technologies)
- [Data Model — Star Schema](#-data-model--star-schema)
- [Pipeline Implementation](#️-pipeline-implementation)
  - [1. Data Simulation](#1-data-simulation)
  - [2. Bronze Layer — Raw Ingestion](#2-bronze-layer--raw-ingestion)
  - [3. Silver Layer — Cleansing & Validation](#3-silver-layer--cleansing--validation)
  - [4. Gold Layer — Star Schema & SCD Type 2](#4-gold-layer--star-schema--scd-type-2)
  - [5. Synapse SQL Pool — External Tables & Views](#5-synapse-sql-pool--external-tables--views)
  - [6. Power BI Dashboard](#6-power-bi-dashboard)
- [Data Quality Handling](#-data-quality-handling)
- [Key Outcomes](#-key-outcomes)
- [How to Run](#-how-to-run)
- [Author](#-author)

---

## 🏢 Business Context

**Client:** Midwest Health Alliance (MHA) — a network of 7 hospitals across the Midwest region.

**Problem:** MHA had no centralised, real-time system to monitor bed occupancy, patient admission and discharge patterns, or department-level load — especially critical during high-demand periods such as seasonal flu outbreaks.

**Requirements from the client:**
- Real-time monitoring of patient admissions to minimise waiting times
- Department-level bottleneck identification across Emergency, Surgery, ICU, and more
- Gender-based and age-based KPIs for demographic insights
- Medallion Architecture (Bronze → Silver → Gold) with schema evolution support
- SCD Type 2 for patient history tracking
- Star schema in Synapse for analytics queries
- Power BI dashboards refreshing in near real-time

> 📄 Full client requirements: [`client_requirements/client_requirements_de.pdf`](client_requirements/client_requirements_de.pdf)

---

## 💡 Solution Overview

An end-to-end real-time data engineering pipeline that:

1. **Simulates** live patient admission and discharge events via a Python producer
2. **Ingests** streaming events through Azure Event Hubs using the Kafka protocol
3. **Processes** data through Bronze → Silver → Gold layers in Databricks (PySpark Structured Streaming)
4. **Stores** curated data in Delta Lake on ADLS Gen2
5. **Loads** Gold layer tables into Azure Synapse SQL Pool as external tables
6. **Exposes** KPI views via Synapse SQL and visualises them in Power BI

---

## 📐 Architecture

![Pipeline Architecture](docs/architecture.png)

> *Event Hubs → Databricks Structured Streaming → Delta Lake (Medallion) → Synapse SQL Pool → Power BI*

---

## 📂 Project Structure

```
hospital-patient-flow-analytics-pipeline/
│
├── client_requirements/
│   └── client_requirements_de.pdf        # Original client brief
│
├── simulator/
│   └── patient_flow_generator.py         # Python Kafka producer — streams patient events to Event Hubs
│
├── databricks-notebooks/
│   ├── 01_bronze_rawdata.py              # Reads Event Hub stream, writes raw JSON to Bronze Delta table
│   ├── 02_silver_cleandata.py            # Parses schema, cleanses data, handles dirty records
│   └── 03_gold_transform.py             # Builds star schema — SCD2 patient dim, department dim, fact table
│
├── sqlpool-queries/
│   ├── SQL_pool_queries.sql              # External table definitions in Synapse SQL Pool
│   └── SQL_views_DDL.sql                # KPI and chart views powering the Power BI dashboard
│
├── powerbi/
│   └── hospital_patient_flow.pbix        # Power BI report connected to Synapse SQL Pool
│
├── git_commands/
│   └── git_bash                          # Git command reference used in this project
│
└── README.md
```

---

## 🛠️ Tools & Technologies

| Tool | Purpose |
|---|---|
| **Azure Event Hubs** | Real-time patient event ingestion via Kafka protocol |
| **Azure Databricks (PySpark)** | Structured Streaming, Delta Lake transformations |
| **Delta Lake** | ACID-compliant storage for Bronze, Silver, Gold layers |
| **Azure Data Lake Storage Gen2** | Unified storage across all Medallion layers |
| **Azure Synapse Analytics** | Serverless SQL Pool — external tables and KPI views |
| **Power BI** | Interactive healthcare analytics dashboard |
| **Python 3.9+** | Data simulation, pipeline scripting |
| **Git** | Version control |

---

## ⭐ Data Model — Star Schema

The Gold layer implements a **star schema** in Synapse SQL Pool for efficient analytical queries:

```
                    ┌─────────────────┐
                    │  dim_patient    │
                    │─────────────────│
                    │ surrogate_key   │
                    │ patient_id      │
                    │ gender          │
                    │ age             │
                    │ effective_from  │  ← SCD Type 2
                    │ effective_to    │
                    │ is_current      │
                    └────────┬────────┘
                             │ patient_sk
              ┌──────────────▼──────────────────┐
              │         fact_patient_flow        │
              │─────────────────────────────────│
              │ fact_id                         │
              │ patient_sk          (FK)         │
              │ department_sk       (FK)         │
              │ admission_time                   │
              │ discharge_time                   │
              │ admission_date                   │
              │ length_of_stay_hours             │
              │ is_currently_admitted            │
              │ bed_id                           │
              │ event_ingestion_time             │
              └──────────────┬──────────────────┘
                             │ department_sk
                    ┌────────▼────────┐
                    │ dim_department  │
                    │─────────────────│
                    │ surrogate_key   │
                    │ department      │
                    │ hospital_id     │
                    └─────────────────┘
```

---

## ⚙️ Pipeline Implementation

### 1. Data Simulation

**File:** [`simulator/patient_flow_generator.py`](simulator/patient_flow_generator.py)

A Python Kafka producer streams synthetic patient events to Azure Event Hubs at 1 event/second. Each event contains patient ID, gender, age, department, admission/discharge timestamps, bed ID, and hospital ID across a network of 7 hospitals.

Dirty data is deliberately injected to test Silver layer data quality handling:
- 5% of records have invalid ages (101–150)
- 5% of records have future admission timestamps

```python
# Sample event payload
{
    "patient_id": "3f2a1b...",
    "gender": "Female",
    "age": 34,
    "department": "Emergency",
    "admission_time": "2026-09-12T08:23:00",
    "discharge_time": "2026-09-12T14:45:00",
    "bed_id": 142,
    "hospital_id": 3
}
```

---

### 2. Bronze Layer — Raw Ingestion

**File:** [`databricks-notebooks/01_bronze_rawdata.py`](databricks-notebooks/01_bronze_rawdata.py)

Reads the raw Event Hubs stream using the Kafka protocol in Databricks Structured Streaming. Data is cast from binary to JSON string and written to a Delta table in ADLS Gen2 with no transformations — exactly as received.

```
Event Hubs (Kafka) → readStream (Kafka format) → Cast to JSON string → writeStream (Delta, append)
```

---

### 3. Silver Layer — Cleansing & Validation

**File:** [`databricks-notebooks/02_silver_cleandata.py`](databricks-notebooks/02_silver_cleandata.py)

Reads the Bronze Delta stream, parses the JSON against an explicit schema, and applies data quality rules:

| Issue | Handling |
|---|---|
| Invalid age (>100) | Replaced with a random valid age (1–90) |
| Future admission timestamps | Replaced with `current_timestamp()` |
| Missing columns due to schema evolution | Added with `null` values via `mergeSchema=true` |
| Type mismatches | Cast `admission_time` and `discharge_time` to `TimestampType` |

---

### 4. Gold Layer — Star Schema & SCD Type 2

**File:** [`databricks-notebooks/03_gold_transform.py`](databricks-notebooks/03_gold_transform.py)

The most complex layer. Reads Silver in batch mode and builds the star schema:

**`dim_patient` — SCD Type 2:**
- Detects attribute changes (gender, age) using SHA-256 hash comparison
- Marks changed rows as `is_current = false` with `effective_to` timestamp
- Inserts new rows for changed or new patients with `is_current = true`

**`dim_department`:**
- Deduplicated by `department` + `hospital_id`
- Fully refreshed on each run

**`fact_patient_flow`:**
- Joins Silver events to current dimension surrogate keys
- Computes `length_of_stay_hours` from admission and discharge timestamps
- Flags `is_currently_admitted` based on whether discharge is in the future
- Partitioned by `admission_date` for query performance

---

### 5. Synapse SQL Pool — External Tables & Views

**Files:** [`sqlpool-queries/SQL_pool_queries.sql`](sqlpool-queries/SQL_pool_queries.sql) | [`sqlpool-queries/SQL_views_DDL.sql`](sqlpool-queries/SQL_views_DDL.sql)

External tables in Synapse point directly to Gold Delta Parquet files in ADLS Gen2 via Managed Identity — no data movement required.

Six analytical views power the Power BI dashboard:

| View | Purpose |
|---|---|
| `vw_bed_occupancy` | Bed occupancy % by gender |
| `vw_bed_turnover_rate` | Bed turnover rate by gender |
| `vw_patient_demographics` | Total active patients by gender |
| `vw_avg_treatment_duration` | Average length of stay by department and gender |
| `vw_patient_volume_trend` | Patient admissions over time by gender |
| `vw_department_inflow` | Active patients per department by gender |
| `vw_overstay_patients` | Patients with length of stay > 50 hours by department |

---

### 6. Power BI Dashboard

**File:** [`powerbi/hospital_patient_flow.pbix`](powerbi/hospital_patient_flow.pbix)

Connected directly to Synapse SQL Pool via live SQL connection. Imports fact and dimension tables with established star schema relationships.

**Dashboard KPIs:**
- Bed Occupancy Rate by department and gender
- Patient Flow Trends (admissions over time)
- Average Length of Stay per department
- Total Active Patients
- Overstay patient count by department

**Interactive Filters:** Gender slicer across all report pages

![Dashboard Overview](powerbi/screenshots/dashboard_overview.png)
![Patient Flow Trends](powerbi/screenshots/patient_flow_trends.png)
![Department KPIs](powerbi/screenshots/department_kpis.png)

---

## ✅ Data Quality Handling

| Layer | Issue | Resolution |
|---|---|---|
| Silver | Age > 100 | Random valid replacement |
| Silver | Future admission timestamps | Replaced with `current_timestamp()` |
| Silver | Schema evolution (new columns) | `mergeSchema=true` + null fill |
| Gold | Duplicate patient records | Window function deduplication by latest admission |
| Gold | Changed patient attributes | SCD Type 2 — new row inserted, old row expired |

---

## 📊 Key Outcomes

- **End-to-end real-time pipeline** from event simulation through to live Power BI dashboard
- **SCD Type 2** implemented in PySpark for full patient history tracking
- **Schema evolution** handled gracefully at the Silver layer — no pipeline downtime on new fields
- **7 analytical SQL views** built in Synapse powering all dashboard KPIs
- **Scalable architecture** — easily adaptable to additional hospital datasets or departments

---

## 🚀 How to Run

**Prerequisites:** Azure subscription, Databricks workspace with Unity Catalog, Synapse Analytics workspace, Power BI Desktop

1. **Set up Event Hubs** — create a namespace and hub named `patient-flow-hub` with a consumer group for Databricks

2. **Run the simulator**
```bash
pip install kafka-python
# Fill in your Event Hubs credentials in patient_flow_generator.py
python simulator/patient_flow_generator.py
```

3. **Run Databricks notebooks in order**
   - Replace all `<<placeholder>>` values with your ADLS and Event Hubs credentials
   - Run `01_bronze_rawdata.py` → `02_silver_cleandata.py` → `03_gold_transform.py`

4. **Create Synapse external tables**
   - Run `sqlpool-queries/SQL_pool_queries.sql` in your Synapse SQL Pool
   - Run `sqlpool-queries/SQL_views_DDL.sql` to create the KPI views

5. **Connect Power BI**
   - Open `powerbi/hospital_patient_flow.pbix`
   - Update the Synapse SQL Pool connection string to your workspace

---

## 👩‍💻 Author
**Pranusha Tirunagari**  
Data Engineer | Azure | Databricks | PySpark | Power BI  
📍 [LinkedIn](https://www.linkedin.com/in/pranusha-tirunagari-a583a63a8/) | [GitHub](https://github.com/Ptirunagari19)
**Pranusha Tirunagari**  
Senior Data Engineer | Azure • Databricks • PySpark • Power BI  
📍 [LinkedIn](https://www.linkedin.com/in/pranusha-tirunagari-a583a63a8/) | [GitHub](https://github.com/Ptirunagari19)
