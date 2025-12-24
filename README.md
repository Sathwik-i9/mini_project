### 1. Project Title

Healthcare Insurance Data Pipeline using Databricks (Bronze–Silver–Gold Architecture)

### 2. Project Overview

This mini project implements an end-to-end data engineering pipeline using Databricks, PySpark, and SQL.
The pipeline processes healthcare patient and insurance data using a multi-batch ingestion approach, applies SCD Type 2 logic, performs data quality checks, and exposes curated data for analytics and dashboards.

### 3. Objectives

Build a scalable data pipeline using Medallion Architecture

Handle incremental data loads (batches)

Implement Slowly Changing Dimension (SCD Type 2)

Track data quality metrics

Create analytical-ready Gold tables

Visualize insights using Databricks Dashboards

### 4. Architecture

Medallion Architecture

Bronze Layer – Raw ingestion

Silver Layer – Cleansed, deduplicated, SCD-enabled

Gold Layer – Aggregated, analytics-ready

Source CSV Files
      ↓
Bronze Layer (Raw)
      ↓
Silver Layer (SCD2 + Cleansing)
      ↓
Gold Layer (Aggregations)
      ↓
Dashboards

### 5. Tech Stack

Platform: Databricks

Language: Python (PySpark), SQL

Storage: Delta Lake, AWS S3

Version Control: GitHub

Visualization: Databricks Dashboards

### 6. Dataset Description

Patient Dataset
Column	Description
patient_id	Unique patient identifier
name	Full name
age	Patient age
address	Indian address
phone_number	Contact number
bill_amount	Hospital bill (₹)
insurance_provider	Insurance company
policy_id	Linked policy
Insurance Dataset
Column	Description
policy_id	Unique policy ID
insurance_provider	Provider name
amount_covered	Coverage amount (₹)
claim_status	Approved / Pending / Rejected

### 7. Batch Processing

Batch 1 → Initial load

Batch 2 → Mix of:

New records

Updated records

Unchanged records

Batch 3 → Further incremental changes

Each batch is processed independently and tracked using batch_id.

### 8. Bronze Layer

Purpose

Load raw CSV files as-is

Add ingestion metadata

Key Columns Added

ingest_time

source_file

row_sequence

batch_id

✔ No transformations
✔ No deletions

### 9. Silver Layer

Purpose

Clean data

Handle nulls

Implement SCD Type 2

Key Features

Deduplication

Null handling

Checksum-based change detection

History preservation

Important Columns
Column	Meaning
transaction_ind	I / U / N
activation_ind	A / U / D
start_date	Record validity start
end_date	Record validity end
checksum_txt	Change detection

### 10. SCD Type 2 Logic
Scenario	transaction_ind	activation_ind
New record	I	A
Updated record	U	U
Unchanged record	N	A
Deleted record	D	D

✔ Historical records preserved
✔ Only one active record (end_date = 9999-12-31)

### 11. Gold Layer
Purpose

Aggregated and analytical datasets

Examples

Claim status distribution

Age group vs insurance provider

Total billed vs covered amount

Duplicate count trends

Null value trends per batch

### 12. Data Quality Checks

Tracked per batch:

Total row count

Duplicate count

Null counts per column

Stored in:

dq_patient_bronze_metrics

dq_insurance_bronze_metrics

### 13. Dashboards

Created using Databricks SQL Dashboards

Claim status bar chart

Age distribution histogram

Null value pie charts

Duplicate trend over batches

### 14. Security & Best Practices

Sensitive credentials ignored via .gitignore

External storage (S3) used

Modular notebooks

Reusable logging utility

### 15. Project Outcomes

End-to-end data pipeline built

SCD Type 2 implemented correctly

Historical tracking enabled

Analytics-ready datasets created

Production-style logging and metrics

### 16. Future Enhancements

Automate batch scheduling

Add unit tests

Introduce CDC from streaming sources

Deploy alerts for DQ failures

### 17. GitHub Repository
https://github.com/Sathwik-i9/mini_project/tree/main/notebooks
