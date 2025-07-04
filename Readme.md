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
    "- **Tables**:\n",
    "  - `bronze_nyc_taxi`\n",
    "  - `silver_nyc_taxi_clean`\n",
    "  - `silver_nyc_taxi_quarantined`\n",
    "  - `gold_daily_location`\n",
    "  - `gold_payment_analysis`\n",
    "\n",
    "> Tables are fully governed and lineage-tracked via Unity Catalog.\n",
    "\n",
    "---\n",
    "\n",
    "## \uD83D\uDD01 Imperative ETL Pipeline (Databricks Workflows)\n",
    "\n",
    "- **Bronze Notebook**: Ingests raw Parquet and appends to `bronze_nyc_taxi`\n",
    "- **Silver Notebook**: Transforms and enforces manual DQ using `assert` statements, then MERGEs into Silver\n",
    "- **Gold Notebook**: Aggregates Silver into daily and payment-type summaries\n",
    "\n",
    "### Orchestration\n",
    "\n",
    "- Implemented as a Databricks multi-task Workflow\n",
    "- Fully parameterized with `input_file_name`\n",
    "- Manages dependencies: Bronze → Silver → Gold\n",
    "\n",
    "---\n",
    "\n",
    "## ⚙️ Declarative ETL Pipeline (DLT)\n",
    "\n",
    "- **Bronze Layer**: Uses Auto Loader with `cloudFiles.format = parquet`\n",
    "- **Silver Layer**: Implements 5+ data expectations using `@dlt.expect` and routes invalid records to a quarantined table\n",
    "- **Gold Layer**: Two DLT tables (`daily_location`, `payment_analysis`) with time-based and location-based aggregations\n",
    "\n",
    "### Pipeline Details\n",
    "\n",
    "- **Pipeline Name**: `NYC_Taxi_DLT_Pipeline`\n",
    "- **Mode**: Triggered\n",
    "- **Target**: `innovateretail_dev.nyc_taxi_pipeline`\n",
    "- **Resources**: Autoscaling (2–6 workers)\n",
    "\n",
    "---\n",
    "\n",
    "## \uD83D\uDCCA Data Quality & Observability\n",
    "\n",
    "### Manual Workflow\n",
    "- Manual assertions stop pipeline execution\n",
    "- Debugging handled via Spark UI\n",
    "\n",
    "### DLT\n",
    "- Native visual quality metrics and row counts\n",
    "- Built-in lineage and quarantine tables\n",
    "- DLT DAG auto-tracks dependencies and outputs\n",
    "\n",
    "---\n",
    "\n",
    "## \uD83E\uDDE0 Comparative Analysis Summary\n",
    "\n",
    "| Feature                | Databricks Workflow                  | Delta Live Tables (DLT)                  |\n",
    "|------------------------|--------------------------------------|------------------------------------------|\n",
    "| Dev Speed              | Manual, verbose                      | Fast, decorator-based                    |\n",
    "| Maintainability        | Fragile on change                    | Auto-managed dependencies                |\n",
    "| Error Handling         | assert statements                    | `@dlt.expect` + quarantine               |\n",
    "| Lineage                | Manual (limited)                     | Visual via Unity Catalog                 |\n",
    "| Infra Management       | User-managed clusters                | DLT auto-managed                         |\n",
    "| Governance             | Minimal                              | Full access control, auditing, lineage   |\n",
    "\n",
    "---\n",
    "\n",
    "## ✅ Deliverables Checklist\n",
    "\n",
    "- [x] Bronze/Silver/Gold implemented in both pipelines\n",
    "- [x] Unity Catalog with schema and table registration\n",
    "- [x] DLT expectations with clean and quarantine outputs\n",
    "- [x] Data Quality Gates\n",
    "- [x] Pipeline Architecture Diagrams (DLT vs Workflow)\n",
    "- [x] Spark UI performance screenshots\n",
    "- [x] Final Comparative Technical Report (PDF + Markdown)\n",
    "\n",
    "---\n",
    "\n",
    "## \uD83D\uDCD8 Author\n",
    "\n",
    "**Jean-Hénock VIAYINON**  \n",
    "Senior Data Engineer | InnovateRetail | Week 3 Submission \n"
   ]
  }
 ],
 "metadata": {
  "application/vnd.databricks.v1+notebook": {
   "computePreferences": null,
   "dashboards": [],
   "environmentMetadata": {
    "base_environment": "",
    "environment_version": "2"
   },
   "inputWidgetPreferences": null,
   "language": "python",
   "notebookMetadata": {
    "pythonIndentUnit": 4
   },
   "notebookName": "ReadMe.md",
   "widgets": {}
  },
  "language_info": {
   "name": "python"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 0
}