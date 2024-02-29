# Airflow Data Orchestration for Logistics Data Pipeline

## Overview
This Apache Airflow project streamlines the ingestion of logistics data from CSV files in Google Cloud Storage (GCS) into Hive tables on Google Cloud Dataproc. Leveraging GCS sensors, the system autonomously detects new file arrivals, initiating the extraction and transformation processes. Through Hive queries, databases and tables are dynamically created and managed, optimizing data organization for efficient analysis. Dynamic partitioning is implemented to enhance data storage scalability and performance. Processed files are moved to the archive folder to avoid duplicate processing. The DAG triggers itself upon completion, enabling ongoing data processing.

## Components
- **Google Cloud Composer**: It is used for orchestrating the data pipeline tasks.
- **Google Cloud Storage (GCS)**: GCS is utilized for storing incoming CSV files and processed data.
- **Google Cloud Dataproc**: Dataproc is used for executing Hive queries and managing Hive tables.


