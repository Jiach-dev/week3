# Week 3 NYC Taxi ETL Pipeline – Comparative Project

## 📌 Project Overview

This project delivers a complete data engineering solution for the NYC Yellow Taxi dataset using both:

- **Imperative ETL with Databricks Workflows**
- **Declarative ETL with Delta Live Tables (DLT)**

It also includes **Unity Catalog governance**, quality enforcement, and a **comparative technical report** to guide best-practice adoption for InnovateRetail.

---

## 🗂️ Directory Structure

```
.
├── notebooks_workflow/
│   ├── 01_workflow_ingest_to_bronze.py
│   ├── 02_workflow_transform_to_silver.py
│   ├── 03_workflow_aggregate_to_gold.py
│
├── dlt_pipeline/
│   └── dlt_pipeline.py
│
├── setup/
│   └── data_staging_notebook.py   # Simulates file ingestion via dbutils.fs.cp
│
├── documentation/
│   ├── W2_NYCTaxi_ETL_Report.md
│   ├── W3_Comparative_ETL_Technical_Report_Clean.pdf
│   ├── W3_Comparative_ETL_Technical_Report_Clean.md
│   └── pipeline_architecture.png  # Imperative & DLT diagrams
│
└── README.md
```

---

## 🛠️ Setup & Configuration

### Unity Catalog Setup

- **Metastore**: `my-meta-store`
- **Region**: `eastus`
- **Catalog**: `innovateretail_dev`
- **Schema**: `nyc_taxi_pipeline`
- **Tables**:
    - `bronze_nyc_taxi`
    - `silver_nyc_taxi_clean`
    - `silver_nyc_taxi_quarantined`
    - `gold_daily_location`
    - `gold_payment_analysis`

> Tables are fully governed and lineage-tracked via Unity Catalog.

---

## 🔁 Imperative ETL Pipeline (Databricks Workflows)

- **Bronze Notebook**: Ingests raw Parquet and appends to `bronze_nyc_taxi`
- **Silver Notebook**: Transforms and enforces manual DQ using `assert` statements, then MERGEs into Silver
- **Gold Notebook**: Aggregates Silver into daily and payment-type summaries

### Orchestration

- Implemented as a Databricks multi-task Workflow
- Fully parameterized with `input_file_name`
- Manages dependencies: Bronze → Silver → Gold

---

## ⚙️ Declarative ETL Pipeline (DLT)

- **Bronze Layer**: Uses Auto Loader with `cloudFiles.format = parquet`
- **Silver Layer**: Implements 5+ data expectations using `@dlt.expect` and routes invalid records to a quarantined table
- **Gold Layer**: Two DLT tables (`daily_location`, `payment_analysis`) with time-based and location-based aggregations

### Pipeline Details

- **Pipeline Name**: `NYC_Taxi_DLT_Pipeline`
- **Mode**: Triggered
- **Target**: `innovateretail_dev.nyc_taxi_pipeline`
- **Resources**: Autoscaling (2–6 workers)

---

## 📊 Data Quality & Observability

### Manual Workflow

- Manual assertions stop pipeline execution
- Debugging handled via Spark UI

### DLT

- Native visual quality metrics and row counts
- Built-in lineage and quarantine tables
- DLT DAG auto-tracks dependencies and outputs

---

## 🧠 Comparative Analysis Summary

| Feature           | Databricks Workflow           | Delta Live Tables (DLT)           |
|-------------------|------------------------------|-----------------------------------|
| Dev Speed         | Manual, verbose               | Fast, decorator-based             |
| Maintainability   | Fragile on change             | Auto-managed dependencies         |
| Error Handling    | assert statements             | `@dlt.expect` + quarantine        |
| Lineage           | Manual (limited)              | Visual via Unity Catalog          |
| Infra Management  | User-managed clusters         | DLT auto-managed                  |
| Governance        | Minimal                       | Full access control, auditing, lineage |

---

## ✅ Deliverables Checklist

- [x] Bronze/Silver/Gold implemented in both pipelines
- [x] Unity Catalog with schema and table registration
- [x] DLT expectations with clean and quarantine outputs
- [x] Data Quality Gates
- [x] Pipeline Architecture Diagrams (DLT vs Workflow)
- [x] Spark UI performance screenshots
- [x] Final Comparative Technical Report (PDF + Markdown)

---

## 📘 Author

**Jean-Hénock VIAYINON**  
Senior Data Engineer | InnovateRetail | Week 3 Submission

