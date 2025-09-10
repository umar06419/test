from datetime import datetime
from google.cloud import bigquery
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.dataform import DataformCreateCompilationResultOperator
from airflow.utils.dates import days_ago
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import requests
import logging
import uuid
from google.cloud import dataform_v1beta1
import ast

DAG_ID = "dependent_dag_dataform"
PROJECT_ID = "premi0537540-gitgbide"
BQ_DATASET = "ds_framework_metadata"
BQ_TABLE = "tm_job_pipeline_actual"
BQ_TABLE_TOTAL = "tm_job_actual"

default_args = {
    'owner': 'salih',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
}

def generate_random_id():
    return str(uuid.uuid4())

def generate_and_push_id(ti, row_id, pipeline_name):
    id_tech_job_actual = generate_random_id()
    ti.xcom_push(
        key=f"id_tech_job_actual_{pipeline_name}_{row_id}",
        value=str(id_tech_job_actual)
    )

def load_dependent_schedule_details(dependent_job_schedule_ids):
    # Normalize input: handle string, empty list, or list with empty string
    if isinstance(dependent_job_schedule_ids, str):
        # Check if it's a JSON array format
        dependent_job_schedule_ids = dependent_job_schedule_ids.strip()
        if dependent_job_schedule_ids.startswith('[') and dependent_job_schedule_ids.endswith(']'):
            try:
                dependent_job_schedule_ids = ast.literal_eval(dependent_job_schedule_ids)
            except (ValueError, SyntaxError) as e:
                logging.error(f"Failed to parse JSON array format: {e}")
                # Fall back to treating as comma-separated string
                dependent_job_schedule_ids = [id.strip() for id in dependent_job_schedule_ids.strip('[]').split(',') if id.strip()]
        else:
            # Treat as comma-separated string or single ID
            dependent_job_schedule_ids = [id.strip() for id in dependent_job_schedule_ids.split(',') if id.strip()]
    
    if not dependent_job_schedule_ids:
        return []
    ids_str = ",".join([f"'{x}'" for x in dependent_job_schedule_ids if x])
    logging.info(f"Loading dependent schedule details for IDs: {ids_str}")
    BQ_QUERY = f"""
        SELECT DISTINCT cd_pipeline_group,lb_email, id_tech_job_schedule, lb_trigger_type, lb_tech_author, lb_repository_id, lb_region, lb_git_comitish,lb_schedule
        FROM ds_framework_metadata.vw_job_schedule_pipeline_details
        WHERE id_tech_job_schedule IN ({ids_str})
            AND lb_repository_id IS NOT NULL AND TRIM(lb_repository_id) != ''
            AND lb_region IS NOT NULL AND TRIM(lb_region) != ''
            AND lb_git_comitish IS NOT NULL AND TRIM(lb_git_comitish) != ''
            AND lb_email IS NOT NULL AND TRIM(lb_email) != ''
            --AND lb_trigger_type = 'Dependent Only'
            AND cd_pipeline_group LIKE '%Level1'
    """
    client = bigquery.Client(project=PROJECT_ID)
    df = client.query(BQ_QUERY).to_dataframe()
    logging.info(f"Loaded dependent schedule details dataframe: {df}")
    return df.to_dict(orient='records')

def process_job(row):
    client = bigquery.Client(project=PROJECT_ID)
    final_list = []
    job_schedule_id = row['id_tech_job_schedule']
    
    # Get parent job dependency
    parent_id_query = f"""
        SELECT distinct id_tech_job_dependency as parent_job_schedule_id
        FROM ds_framework_metadata.tm_job_schedule
        WHERE id_tech_job_schedule = '{job_schedule_id}'
            --AND lb_trigger_type = 'Dependent Only'
    """
    parent_df = client.query(parent_id_query).to_dataframe()
    
    # Check parent job status if parent exists
    parent_result = []
    if not parent_df.empty and parent_df['parent_job_schedule_id'].iloc[0] is not None:
        check_parent_query = f"""
            SELECT lb_status, nb_retires
            FROM (
                SELECT lb_status, nb_retires,
                    ROW_NUMBER() OVER (
                        PARTITION BY id_tech_job_schedule 
                        ORDER BY dt_actual_start DESC
                    ) AS row_num
                FROM ds_framework_metadata.tm_job_actual
                WHERE id_tech_job_schedule = '{parent_df['parent_job_schedule_id'].iloc[0]}'
            ) subquery
            WHERE row_num = 1
        """
        parent_result = client.query(check_parent_query).to_dataframe().to_dict(orient='records')
    
    # Check current job status
    check_current_query = f"""
        SELECT lb_status, nb_retires
        FROM (
            SELECT lb_status, nb_retires,
                ROW_NUMBER() OVER (
                    PARTITION BY id_tech_job_schedule 
                    ORDER BY dt_actual_start DESC
                ) AS row_num
            FROM ds_framework_metadata.tm_job_actual
            WHERE id_tech_job_schedule = '{job_schedule_id}'
        ) subquery
        WHERE row_num = 1
    """
    current_result = client.query(check_current_query).to_dataframe().to_dict(orient='records')
    logging.info(f"Check query result for job_schedule_id {job_schedule_id}: {current_result}")
    
    # Determine if parent job is successful (or no parent exists)
    parent_successful = not parent_result or parent_result[0]['lb_status'] == 'SUCCEEDED'
    
    # Logic: Process job only if no current execution exists AND parent is successful
    if not current_result and parent_successful:
        final_list.append({
            'job_schedule_id': row['id_tech_job_schedule'],
            'cd_pipeline_group': row['cd_pipeline_group'],
            'lb_tech_author': row['lb_tech_author'],
            'lb_repository_id': row['lb_repository_id'],
            'lb_region': row['lb_region'],
            'lb_git_comitish': row['lb_git_comitish'],
            'lb_email': row['lb_email'],
            'cd_pipeline': row['lb_schedule'],
            'lb_trigger_type': row['lb_trigger_type']
        })
    # If current job exists, check its status
    elif current_result:
        current_status = current_result[0]['lb_status']
        
        # If current job succeeded AND parent is successful, add to final list
        if current_status == 'SUCCEEDED' and parent_successful:
            final_list.append({
                'job_schedule_id': row['id_tech_job_schedule'],
                'cd_pipeline_group': row['cd_pipeline_group'],
                'lb_tech_author': row['lb_tech_author'],
                'lb_repository_id': row['lb_repository_id'],
                'lb_region': row['lb_region'],
                'lb_git_comitish': row['lb_git_comitish'],
                'lb_email': row['lb_email'],
                'cd_pipeline': row['lb_schedule'],
                'lb_trigger_type': row['lb_trigger_type']
            })
        # If current job failed OR parent failed, handle error
        elif current_status == 'FAILED' or not parent_successful:
            error_message = f"The previous Job {job_schedule_id} was not in success state."
            if not parent_successful:
                parent_id = parent_df['parent_job_schedule_id'].iloc[0] if not parent_df.empty else 'Unknown'
                error_message = f"Parent job {parent_id} failed, skipping dependent job {job_schedule_id}."
            
            logging.info(error_message)
            
            # Insert error info to tm_job_actual table
            insert_query = f"""
                INSERT INTO ds_framework_metadata.tm_job_actual
                (id_tech_job_actual, id_tech_job_schedule, lb_status, dt_actual_start, dt_actual_end, lb_error)
                VALUES (
                    '{generate_random_id()}',
                    '{job_schedule_id}',
                    'SKIPPED',
                    CURRENT_TIMESTAMP(),
                    CURRENT_TIMESTAMP(),
                    '{error_message}'
                )
            """
            try:
                client.query(insert_query).result()
                logging.info(f"Inserted error info for job_schedule_id {job_schedule_id}")
            except Exception as e:
                logging.error(f"Failed to insert error info: {e}")

            # Fire webhook to Teams with job info
            payload = {
                "type": "message",
                "attachments": [
                    {
                        "contentType": "application/vnd.microsoft.card.adaptive",
                        "content": {
                            "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                            "type": "AdaptiveCard",
                            "version": "1.4",
                            "body": [
                                {
                                    "type": "TextBlock",
                                    "text": "🚨 Composer Workflow DataForm Job Notification",
                                    "weight": "Bolder",
                                    "size": "Medium",
                                    "color": "Default"
                                },
                                {
                                    "type": "TextBlock",
                                    "text": f"The job with schedule ID **{job_schedule_id}** has failed.",
                                    "wrap": True
                                },
                                {
                                    "type": "FactSet",
                                    "facts": [
                                        {"title": "State:", "value": 'SKIPPED'},
                                        {"title": "Load Type:", "value": row['lb_schedule']},
                                        {"title": "Job Schedule ID:", "value": job_schedule_id},
                                        {"title": "Trigger Type:", "value": row['lb_trigger_type']},
                                        {"title": "Message:", "value": f"The previous Job {job_schedule_id} was not in success state."},
                                        {"title": "🤖 Contact Author:", "value": row['lb_tech_author']},
                                    ]
                                }
                            ],
                            "actions": [
                                {
                                    "type": "Action.OpenUrl",
                                    "title": "🔍 View Logs",
                                    "url": "https://console.cloud.google.com/bigquery?ws=!1m5!1m4!4m3!1spremi0537540-gitgbide!2sds_framework_metadata!3stm_job_actual"
                                }
                            ]
                        }
                    }
                ]
            }

            try:
                response = requests.post(row['lb_email'], json=payload)
                if response.status_code == 200:
                    logging.info(f"Webhook notification sent successfully for job {job_schedule_id}.")
                else:
                    logging.error(f"Failed to send webhook notification. Status code: {response.status_code}, Response: {response.text}")
            except Exception as e:
                logging.error(f"Error sending webhook notification: {e}")
        else:
            logging.info(f"Job {job_schedule_id} is in {current_status} state, not processing.")
    
    logging.info(f"Final list for job_schedule_id {job_schedule_id}: {final_list}")
    return final_list

def loop_all_jobs(list_rows):
    final_data = []
    for row in list_rows:
        final_data.extend(process_job(row))
    return final_data

def create_workflow_and_capture_id(ti, project_id, region, repository_id, compilation_result_name, invocation_tag, row_id):
    client = dataform_v1beta1.DataformClient()
    if not compilation_result_name:
        raise ValueError("Error: Compilation result not found")
    workflow_invocation = {
        "compilation_result": compilation_result_name,
        "invocation_config": {"included_tags": [invocation_tag],
                            "transitive_dependents_included": True},
    }
    parent = f"projects/{project_id}/locations/{region}/repositories/{repository_id}"
    invocation_response = client.create_workflow_invocation(parent=parent, workflow_invocation=workflow_invocation)
    full_invocation_path = invocation_response.name
    invocation_id = full_invocation_path.split("/")[-1]
    ti.xcom_push(key=f"workflow_invocation_{row_id}" , value=invocation_id)
    ti.xcom_push(key=f"start_time_{row_id}", value=str(datetime.utcnow()))


with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    # max_active_runs=20,
    tags=['dataform', 'orchestration', 'dependent'],
    params={
        "dependent_job_schedule_ids": "Enter comma-separated IDs or JSON array format like idx1,idx2,idx3 or ['idx1','idx2','idx3']"
    }
) as dag:

    def orchestrate_dependent_jobs(**context):
        conf = context['dag_run'].conf or {}
        params = context.get('params', {})
        
        # Get dependent_job_schedule_ids from multiple sources
        dependent_job_schedule_ids = None
        
        # Priority 1: From DAG run configuration (triggered by another DAG)
        if conf.get('dependent_job_schedule_ids'):
            dependent_job_schedule_ids = conf.get('dependent_job_schedule_ids')
            logging.info(f"Using dependent_job_schedule_ids from DAG run conf: {dependent_job_schedule_ids}")
        
        # Priority 2: From UI parameter (manual entry)
        elif params.get('dependent_job_schedule_ids'):
            param_value = params.get('dependent_job_schedule_ids')
            
            # Handle different input formats
            if isinstance(param_value, list):
                # Already a list (from programmatic trigger)
                dependent_job_schedule_ids = param_value
                logging.info(f"Using dependent_job_schedule_ids as list from params: {dependent_job_schedule_ids}")
            elif isinstance(param_value, str) and param_value.strip() and param_value != "Enter comma-separated IDs or JSON array format like idx1,idx2,idx3 or ['idx1','idx2','idx3']":
                # String input from UI
                param_value = param_value.strip()
                try:
                    # Try to parse as JSON array first
                    if param_value.startswith('[') and param_value.endswith(']'):
                        dependent_job_schedule_ids = ast.literal_eval(param_value)
                    else:
                        # Split comma-separated values
                        dependent_job_schedule_ids = [id.strip() for id in param_value.split(',') if id.strip()]
                    logging.info(f"Using dependent_job_schedule_ids parsed from string params: {dependent_job_schedule_ids}")
                except Exception as e:
                    logging.error(f"Failed to parse parameter value '{param_value}': {e}")
                    raise ValueError(f"Invalid format for dependent_job_schedule_ids. Use comma-separated IDs like 'id1,id2,id3' or JSON array format like ['id1','id2','id3']. Error: {e}")
        
        # Validate we have the required parameter
        if not dependent_job_schedule_ids:
            logging.warning("No dependent_job_schedule_ids provided from any source.")
            logging.info("Available sources:")
            logging.info(f"  - DAG run conf: {conf}")
            logging.info(f"  - Params: {params}")
            logging.info("Please provide dependent_job_schedule_ids either:")
            logging.info("  1. Through DAG trigger configuration")
            logging.info("  2. Through UI parameter as comma-separated IDs or JSON array")
            return
        
        logging.info(f"Final dependent_job_schedule_ids to process: {dependent_job_schedule_ids}")

        list_all = load_dependent_schedule_details(dependent_job_schedule_ids)
        logging.info(f"Loaded dependent job schedule details list_all: {list_all}")
        pipeline_data = loop_all_jobs(list_all)
        logging.info(f"Pipeline data to process: {pipeline_data}")

        for id, row in enumerate(pipeline_data):
            logging.info(f"Processing row {id}: {row}")
            REPOSITORY_ID = row['lb_repository_id']
            REGION = row['lb_region']
            GIT_COMMITISH = row['lb_git_comitish']
            WEBHOOK_URL = row['lb_email']

            # Generate job actual ID
            ti = context['ti']
            generate_and_push_id(ti, id, row['cd_pipeline'])
            id_tech_job_actual = ti.xcom_pull(key=f"id_tech_job_actual_{row['cd_pipeline']}_{id}")

            # Create compilation result
            compilation_result_operator = DataformCreateCompilationResultOperator(
                task_id=f"create_compilation_result_{row['cd_pipeline']}_{id}",
                project_id=PROJECT_ID,
                region=REGION,
                repository_id=REPOSITORY_ID,
                compilation_result={
                    "git_commitish": GIT_COMMITISH,
                    "code_compilation_config": {
                        "vars": {
                            "var_dt_job_start": context['ts'].replace('T', ' ').replace('+00:00', ''),
                            "var_job_schedule_id": row["job_schedule_id"],
                            "var_job_actual_id": id_tech_job_actual
                        }
                    },
                },
                trigger_rule="all_done",
                dag=dag
            )
            compilation_result = compilation_result_operator.execute(context)
            compilation_result_name = compilation_result['name']

            # Create workflow and capture id
            create_workflow_and_capture_id(
                ti=ti,
                project_id=PROJECT_ID,
                row_id=id,
                region=REGION,
                repository_id=REPOSITORY_ID,
                compilation_result_name=compilation_result_name,
                invocation_tag=row["cd_pipeline_group"]
            )
            workflow_invocation_id = ti.xcom_pull(key=f"workflow_invocation_{id}")
            start_time = ti.xcom_pull(key=f"start_time_{id}")

            # Trigger reporting DAG
            trigger_monitoring_dag = TriggerDagRunOperator(
                task_id=f"trigger_monitoring_dag_{row['cd_pipeline']}_{id}",
                trigger_dag_id="monitoring_dag_dataform",
                conf={
                    "project_id": PROJECT_ID,
                    "region": REGION,
                    "repository_id": REPOSITORY_ID,
                    "row_id": id,
                    "pipeline_name": row["cd_pipeline"],
                    "id_tech_job_schedule": row["job_schedule_id"],
                    "id_tech_job_actual": id_tech_job_actual,
                    "BQ_DATASET": BQ_DATASET,
                    "BQ_TABLE": BQ_TABLE,
                    "BQ_TABLE_TOTAL": BQ_TABLE_TOTAL,
                    "WEBHOOK_URL": WEBHOOK_URL,
                    "author_name": row["lb_tech_author"],
                    "cd_dag_id": DAG_ID,
                    "start_time": start_time,
                    "workflow_invocation_id": workflow_invocation_id,
                    "compilation_result_name": compilation_result_name,
                    "compilation_result_id": compilation_result_name.split('/')[-1],
                    'lb_trigger_type': row['lb_trigger_type'],
                    "lb_schedule": row['cd_pipeline']
                },
                trigger_rule="all_done",
                dag=dag
            )
            trigger_monitoring_dag.execute(context)

    orchestrate = PythonOperator(
        task_id="orchestrate_dependent_jobs",
        python_callable=orchestrate_dependent_jobs,
        provide_context=True,
        trigger_rule="all_done"
    )
    orchestrate