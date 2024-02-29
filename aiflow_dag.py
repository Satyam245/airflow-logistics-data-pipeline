from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitHiveJobOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago
from airflow.utils import timezone

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'hive_load_airflow_dag1',
    default_args=default_args,
    description='Load logistics data into Hive on GCP Dataproc',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    tags=['example'],
)

# Sense the new file in GCS
sense_logistics_file = GCSObjectsWithPrefixExistenceSensor(
    task_id='sense_logistics_file',
    bucket='logistics-raw-gds',
    prefix='input_data/logistics_',
    mode='poke',
    timeout=300,
    poke_interval=30,
    dag=dag
)

# Create Hive Database if not exists
create_hive_database = DataprocSubmitHiveJobOperator(
    task_id="create_hive_database",
    query="CREATE DATABASE IF NOT EXISTS logistics_db;",
    cluster_name='your_dataproc_cluster_name',
    region='us-central1',
    project_id='your_project_id',
    dag=dag
)

# Create main Hive table
create_hive_table = DataprocSubmitHiveJobOperator(
    task_id="create_hive_table",
    query="""
        CREATE EXTERNAL TABLE IF NOT EXISTS logistics_db.logistics_data (
            delivery_id INT,
            `date` STRING,
            origin STRING,
            destination STRING,
            vehicle_type STRING,
            delivery_status STRING,
            delivery_time STRING
        )
        ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
        STORED AS TEXTFILE
        LOCATION 'gs://logistics-raw-gds/input_data/'
        tblproperties('skip.header.line.count'='1');
    """,
    cluster_name='your_dataproc_cluster_name',
    region='us-central1',
    project_id='your_project_id',
    dag=dag
)

# Create partitioned Hive table
create_partitioned_table = DataprocSubmitHiveJobOperator(
    task_id="create_partitioned_table",
    query="""
        CREATE TABLE IF NOT EXISTS logistics_db.logistics_data_partitioned (
            delivery_id INT,
            origin STRING,
            destination STRING,
            vehicle_type STRING,
            delivery_status STRING,
            delivery_time STRING
        )
        PARTITIONED BY (`date` STRING)
        STORED AS TEXTFILE;
    """,
    cluster_name='your_dataproc_cluster_name',
    region='us-central1',
    project_id='your_project_id',
    dag=dag
)

# Set Hive properties for dynamic partitioning and load data
set_hive_properties_and_load_partitioned = DataprocSubmitHiveJobOperator(
    task_id="set_hive_properties_and_load_partitioned",
    query=f"""
        SET hive.exec.dynamic.partition = true;
        SET hive.exec.dynamic.partition.mode = nonstrict;

        INSERT INTO logistics_db.logistics_data_partitioned PARTITION(`date`)
        SELECT delivery_id, origin,destination, vehicle_type, delivery_status, delivery_time, `date` FROM logistics_db.logistics_data;
    """,
    cluster_name='your_dataproc_cluster_name',
    region='us-central1',
    project_id='your_project_id',
    dag=dag
)

# Move processed files to archive bucket
archive_processed_file = BashOperator(
    task_id='archive_processed_file',
    bash_command=f"gsutil -m mv gs://logistics-raw-gds/input_data/logistics_*.csv gs://logistics-archive-gds/",
    dag=dag
)

# Task to trigger DAG again
trigger_downstream_dag = TriggerDagRunOperator(
    task_id="trigger_dag_again",
    trigger_dag_id="hive_load_airflow_dag1",
    execution_date=timezone.utcnow(),  # Use timezone-aware current time
    dag=dag
)

sense_logistics_file >> create_hive_database >> create_hive_table >> create_partitioned_table >> set_hive_properties_and_load_partitioned >> archive_processed_file >> trigger_downstream_dag
