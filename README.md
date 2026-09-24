# NYC Taxi Data Engineering Pipeline

An end-to-end data engineering pipeline built using PySpark, Apache Spark, Databricks, and Delta Lake. The project processes NYC TLC Yellow Taxi trip data for January 2026 using a Bronze-Silver-Gold lakehouse architecture.

## Architecture

NYC TLC Parquet Data
        ↓
Bronze / Raw
        ↓
Data Quality Validation
        ↓
Silver / Cleaned & Transformed
        ↓
Delta Lake
        ↓
Gold
 ├── Daily Performance
 ├── Hourly Performance
 ├── Location Performance
 ├── Peak Hour Ranking
 └── Business Insights

## Technologies

- Python
- PySpark
- Apache Spark
- Databricks
- Delta Lake
- Parquet
- Git / GitHub

## Dataset

Source: NYC TLC Yellow Taxi Trip Data

Processing period: January 2026

The raw dataset contains approximately 3.72 million trip records.

## Pipeline

### 1. Bronze Layer

The raw Parquet dataset is ingested into a PySpark DataFrame.

Initial data-quality checks identify:

- Missing pickup/drop-off timestamps
- Invalid passenger counts
- Invalid trip distances
- Negative fares
- Negative total amounts

### 2. Silver Layer

Invalid records are filtered and additional analytical columns are created:

- `trip_duration_minutes`
- `pickup_date`
- `pickup_hour`
- `revenue_per_mile`
- `tip_percentage`

The cleaned Silver dataset contains approximately 2.51 million records.

The cleaned data is stored using Delta Lake.

### 3. Gold Layer

The pipeline produces analytical datasets for:

- Daily trip performance
- Hourly demand and revenue
- Pickup-location performance
- Peak-hour demand ranking
- Business insights

Spark Window Functions are used to rank peak-demand hours.

## Data Quality

The pipeline validates the data before and after transformation.

Silver validation confirms:

- Valid passenger counts
- Valid trip distances
- Non-negative fares
- Non-negative total amounts
- Positive trip durations

## Key Engineering Concepts

- ETL / ELT pipeline design
- Bronze-Silver-Gold architecture
- Data quality validation
- Distributed processing with Spark
- PySpark DataFrame transformations
- Delta Lake storage
- Aggregations and analytical transformations
- Spark Window Functions
- Reproducible pipeline configuration

## Repository Structure

```text
nyc-taxi-lakehouse/
│
├── NYC_Taxi_PySpark_Pipeline.py
└── README.md
How to Run

The pipeline is designed to run in Databricks.

Upload the NYC TLC Yellow Taxi Parquet dataset to a Databricks Volume.
Update SOURCE_PATH if required.
Run NYC_Taxi_PySpark_Pipeline.py.
The pipeline creates Silver and Gold Delta outputs.

The raw dataset is not included in this repository because of its size.

Project Outcome

This project demonstrates an end-to-end modern data engineering workflow using PySpark and Databricks, from raw data ingestion and quality validation through transformation, Delta Lake storage, analytical aggregation, and business-level insights.
