from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from google.cloud import bigquery, storage
import pandas as pd
import re
import logging
import tempfile
import os

# DAG configuration
PROJECT_ID = "premi0537540-gitgbide"
METADATA_DATASET = "ds_framework_metadata"
METADATA_TABLE = "vw_source_target"
LOG_TABLE = f"{PROJECT_ID}.{METADATA_DATASET}.tm_job_actual"


default_args = {
    'owner': 'Umar',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
}

def read_metadata(**context):
    bq_client = bigquery.Client(project=PROJECT_ID)
    query = f"""
        SELECT *
        FROM `{PROJECT_ID}.{METADATA_DATASET}.{METADATA_TABLE}`
        WHERE cd_source_type = 'File'
    """
    df = bq_client.query(query).to_dataframe()
    context['ti'].xcom_push(key='metadata', value=df.to_dict('records'))

def get_latest_file_uri(gcs_client, gcs_uri_prefix, filename_pattern):
    bucket_name = gcs_uri_prefix.replace("gs://", "").split("/")[0]
    prefix_path = "/".join(gcs_uri_prefix.replace("gs://", "").split("/")[1:])
    bucket = gcs_client.bucket(bucket_name)
    blobs = list(gcs_client.list_blobs(bucket, prefix=prefix_path))
    if not blobs:
        raise Exception(f"No files found in path {gcs_uri_prefix}")
    filtered = [blob for blob in blobs if re.search(filename_pattern, blob.name)]
    if not filtered:
        raise Exception(f"No files matching pattern '{filename_pattern}' in {gcs_uri_prefix}")
    latest_blob = sorted(filtered, key=lambda x: x.updated)[-1]
    return f"gs://{bucket_name}/{latest_blob.name}"

def convert_excel_to_csv(gcs_client, gcs_uri, temp_bucket, temp_prefix):
    bucket_name = gcs_uri.replace("gs://", "").split("/")[0]
    blob_path = "/".join(gcs_uri.replace("gs://", "").split("/")[1:])
    bucket = gcs_client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as temp_excel:
        blob.download_to_filename(temp_excel.name)
        temp_excel_path = temp_excel.name
    df = pd.read_excel(temp_excel_path)
    temp_csv_path = temp_excel_path.replace(".xlsx", ".csv")
    df.to_csv(temp_csv_path, index=False)
    temp_blob_name = f"{temp_prefix}/{os.path.basename(temp_csv_path)}"
    temp_bucket_obj = gcs_client.bucket(temp_bucket)
    temp_blob = temp_bucket_obj.blob(temp_blob_name)
    temp_blob.upload_from_filename(temp_csv_path)
    os.remove(temp_excel_path)
    os.remove(temp_csv_path)
    return f"gs://{temp_bucket}/{temp_blob_name}"

def process_metadata(**context):
    bq_client = bigquery.Client(project=PROJECT_ID)
    gcs_client = storage.Client(project=PROJECT_ID)
    metadata = context['ti'].xcom_pull(key='metadata', task_ids='read_metadata')
    processed_count = 0
    for row in metadata:
        file_type = row['cd_file_type'].lower()
        file_pattern = row['lb_source']
        gcs_prefix = row['lb_source_dataset']
        target_project = row['lb_target_project']
        target_dataset = row['lb_target_dataset']
        original_table = row['lb_target']
        target_table = f"ext_{original_table}"
        try:
            latest_file_uri = get_latest_file_uri(gcs_client, gcs_prefix, file_pattern)
            file_name = latest_file_uri.split("/")[-1]
            if file_type == 'parquet':
                ddl = f"""
                CREATE OR REPLACE EXTERNAL TABLE `{target_project}.{target_dataset}.{target_table}`
                OPTIONS (
                  format = 'PARQUET',
                  uris = ['{latest_file_uri}']
                );
                """
            elif file_type == 'csv':
                ddl = f"""
                CREATE OR REPLACE EXTERNAL TABLE `{target_project}.{target_dataset}.{target_table}`
                OPTIONS (
                  format = 'CSV',
                  skip_leading_rows = 1,
                  field_delimiter = ',',
                  uris = ['{latest_file_uri}']
                );
                """
            elif file_type in ['xlsx', 'xls', 'excel']:
                temp_bucket = bucket_name  # Use the same bucket or a temp bucket
                temp_prefix = "temp_csv"
                csv_gcs_uri = convert_excel_to_csv(gcs_client, latest_file_uri, temp_bucket, temp_prefix)
                ddl = f"""
                CREATE OR REPLACE EXTERNAL TABLE `{target_project}.{target_dataset}.{target_table}`
                OPTIONS (
                  format = 'CSV',
                  skip_leading_rows = 1,
                  field_delimiter = ',',
                  uris = ['{csv_gcs_uri}']
                );
                """
            else:
                logging.warning(f"Unsupported file type: {file_type} for table {original_table}")
                continue
            job = bq_client.query(ddl)
            job.result()
            status = 'SUCCEEDED'
            error_message = ''
        except Exception as e:
            status = 'FAILED'
            error_message = str(e).replace("'", "")[:1000]
        # Log job status
        job_time = datetime.utcnow().isoformat()
        insert_query = f"""
        INSERT INTO `{LOG_TABLE}` (
            id_tech_job_actual, dt_actual_start, dt_actual_end, workflow_id,
            lb_status, lb_dag, fl_current, lb_tech_author, compilation_result_id,
            dt_tech_insert, dt_tech_update, lb_error
        ) VALUES (
            GENERATE_UUID(), TIMESTAMP('{job_time}'), TIMESTAMP('{job_time}'),
            '{file_name}', '{status}', 'external_table_creation_dag', TRUE,
            'umar.shaikh@capgemini.com', '{latest_file_uri}', TIMESTAMP('{job_time}'), TIMESTAMP('{job_time}'),
            '{error_message}'
        );
        """
        bq_client.query(insert_query).result()
        processed_count += 1
    logging.info(f"Finished processing. Attempted to log status for {processed_count} files.")

with DAG(
    dag_id='bq_external_table_from_gcs',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['bigquery', 'gcs', 'external-table'],
) as dag:

    read_metadata_task = PythonOperator(
        task_id='read_metadata',
        python_callable=read_metadata,
        provide_context=True,
    )

    process_metadata_task = PythonOperator(
        task_id='process_metadata',
        python_callable=process_metadata,
        provide_context=True,
    )

    read_metadata_task >> process_metadata_task