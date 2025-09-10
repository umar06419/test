from datetime import datetime
from google.cloud import bigquery
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.google.cloud.operators.dataform import DataformCreateCompilationResultOperator
from airflow.utils.dates import days_ago
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import requests
import logging
import uuid
from google.cloud import dataform_v1beta1

# Constants
DAG_ID = "main_dag_dataform"
PROJECT_ID = "premi0537540-gitgbide"
# BigQuery dataset and table names
BQ_DATASET = "ds_framework_metadata" 
BQ_TABLE = "tm_job_pipeline_actual"
BQ_TABLE_TOTAL = "tm_job_actual" 

# The purpose of the task is to create a function that reads the data of constanats variables dynamically

default_args = {
    'owner': 'rida',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

def generate_random_id():
    return str(uuid.uuid4())

def generate_and_push_id(ti, row_id, pipeline_name, task_group, pipeline_data):
    id_tech_job_actual = generate_random_id()
    logging.info(f"The final pipelione list is : {pipeline_data}")
    ti.xcom_push(
        key=f"id_tech_job_actual_{pipeline_name}_{row_id}",
        value=str(id_tech_job_actual)
    )

# Load and process jobs
def load_schedule_details():
    BQ_QUERY = """SELECT DISTINCT cd_pipeline_group,lb_email, id_tech_job_schedule, lb_schedule, lb_trigger_type, lb_tech_author, tm_execution
    FROM ds_framework_metadata.vw_job_schedule_pipeline_details WHERE cd_pipeline_group LIKE '%Level1' and fl_active = True
    AND lb_trigger_type IN ('Schedule and Dependent','Schedule Only')"""
    client = bigquery.Client(project=PROJECT_ID)
    df = client.query(BQ_QUERY).to_dataframe()
    logging.info(f"the dataframe is: {df} ")
    return df.to_dict(orient='records')

def process_job(row):
    client = bigquery.Client(project=PROJECT_ID)
    final_list = []
    job_schedule_id = row['id_tech_job_schedule']
    parent_df = None
    
    if row['lb_trigger_type'] == 'Schedule and Dependent':
        # Get parent job_id if trigger type is 'Schedule and Dependent'
        parent_id_query = f"""
            SELECT distinct id_tech_job_dependency as parent_job_schedule_id
            FROM ds_framework_metadata.tm_job_schedule
            WHERE id_tech_job_schedule = '{job_schedule_id}'
                AND lb_trigger_type = 'Schedule and Dependent'
        """
        parent_df = client.query(parent_id_query).to_dataframe()
    
    # Check parent job status if parent exists
    parent_result = []
    if parent_df is not None and not parent_df.empty and parent_df['parent_job_schedule_id'].iloc[0] is not None:
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
            'cd_pipeline': row['lb_schedule'],
            'tm_execution': row['tm_execution'],
            'lb_tech_author': row['lb_tech_author'],
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
                'cd_pipeline': row['lb_schedule'],
                'tm_execution': row['tm_execution'],
                'lb_tech_author': row['lb_tech_author'],
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
    try:
        invocation_response = client.create_workflow_invocation(parent=parent, workflow_invocation=workflow_invocation)
        full_invocation_path = invocation_response.name
        logging.info(f"Workflow Invocation Created: {full_invocation_path}")
        invocation_id = full_invocation_path.split("/")[-1]
        logging.info(f"Workflow Invocation Created: {invocation_id}")
    except Exception as e:
        logging.info(f"Workflow invocation failed: {e}")
    ti.xcom_push(key=f"workflow_invocation_{row_id}" , value=invocation_id)
    ti.xcom_push(key=f"start_time_{row_id}", value=str(datetime.utcnow()))  # Store start time


def is_time_in_window(target_times, window_minutes=4):
    """
    Check if the current time falls within a window around any of the target times.  
    - `target_times`: list of strings in "HH:MM" format  
    - `window_minutes`: tolerance in minutes (before/after)
    """
    now = datetime.now()
    for t in target_times:
        target = datetime.strptime(t, "%H:%M").replace(
            year=now.year, month=now.month, day=now.day
        )
        if abs((now - target).total_seconds()) <= window_minutes * 60:
            return True
    return False

# Function to load dynamic constants based on domain type and job schedule ID
def load_dynamic_constants(domain_type, job_schedule_id):
    query = f"""
            SELECT 
        id_tech_job_schedule,
        lb_repository_id, 
        lb_region, 
        lb_git_comitish, 
        lb_email, 
        lb_schedule,
        lb_trigger_type     
    FROM ds_framework_metadata.vw_job_schedule_pipeline_details
    WHERE fl_active = TRUE
      AND LOWER(lb_schedule) LIKE '%{domain_type.lower()}%'
      AND lb_repository_id IS NOT NULL AND TRIM(lb_repository_id) != ''
      AND lb_region IS NOT NULL AND TRIM(lb_region) != ''
      AND lb_git_comitish IS NOT NULL AND TRIM(lb_git_comitish) != ''
      AND lb_email IS NOT NULL AND TRIM(lb_email) != ''
      AND id_tech_job_schedule = '{job_schedule_id}'
    """
    bq_client = bigquery.Client(project=PROJECT_ID)
    
    df = bq_client.query(query).to_dataframe()
    if df.empty:
        logging.info(f"No active schedule found for domain type: {domain_type}") 

    try:
        df_final = df.to_dict(orient='records')[0] 
        return df_final
    except:
        logging.info("load_dynamic_constants is empty")
        return None
        

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    schedule_interval='*/5 * * * *',
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=['dataform', 'orchestration']
) as dag:
    
    # Load dynamic constants
    domain_list = ['sales']

    for domain in domain_list:
        list_all = load_schedule_details()
        pipeline_data = loop_all_jobs(list_all)

        # for loop for task group
        with TaskGroup(group_id=f"{domain}_task_group") as group:
            for id, row in enumerate(pipeline_data):
                # Load dynamic constants for the domain
                config = load_dynamic_constants(domain, row['job_schedule_id'])
                if config is None:
                    continue  # Skip if no config found
                REPOSITORY_ID = config['lb_repository_id']
                REGION = config['lb_region']
                GIT_COMMITISH = config['lb_git_comitish']
                WEBHOOK_URL = config['lb_email']
                # check window time exec  row['tm_execution']
                # execution_hours = ['11:40']
                if is_time_in_window(row['tm_execution'], window_minutes=4):
                    generate_id_task = PythonOperator(
                        task_id=f"generate_id_{row['cd_pipeline']}_{id}",
                        python_callable=generate_and_push_id,
                        op_kwargs={
                            'row_id': id,
                            'pipeline_name': row['cd_pipeline'],
                            'task_group': f"{domain}_task_group",
                            'pipeline_data': pipeline_data
                        }
                    )
                    

                    create_compilation_result = DataformCreateCompilationResultOperator(
                    task_id=f"create_compilation_result_{row['cd_pipeline']}_{id}",
                    project_id=PROJECT_ID,
                    region=REGION,
                    repository_id=REPOSITORY_ID,
                    compilation_result={
                        "git_commitish": GIT_COMMITISH,
                        "code_compilation_config": {
                            "vars": {
                                "var_dt_job_start": "{{ ts | replace('T', ' ') | replace('+00:00', '') }}",  
                                "var_job_schedule_id": row["job_schedule_id"],
                                "var_job_actual_id": "{{ task_instance.xcom_pull(task_ids='"  + f"{domain}_task_group.generate_id_{row['cd_pipeline']}_{id}" + "', key='id_tech_job_actual_" + f"{row['cd_pipeline']}_{id}" + "') }}"
                            }
                        },
                    },
                    trigger_rule="all_done",
                    )

                    invocation_workflow = PythonOperator(
                        task_id=f"create_workflow_and_capture_id_{row['cd_pipeline']}_{id}",
                        python_callable=create_workflow_and_capture_id,
                        op_kwargs={
                            'project_id': PROJECT_ID,
                            'row_id': id,
                            'region': REGION,
                            'repository_id': REPOSITORY_ID,
                            'compilation_result_name': "{{ task_instance.xcom_pull(task_ids='"+ f"{domain}_task_group.create_compilation_result_{row['cd_pipeline']}_{id}" + "')['name'] }}",
                            'invocation_tag': row["cd_pipeline_group"],
                        },
                        provide_context=True,
                        trigger_rule="all_done",
                    )
                    trigger_monitoring_dag = TriggerDagRunOperator(
                        task_id=f"trigger_monitoring_dag_{row['cd_pipeline']}_{id}",
                        trigger_dag_id="monitoring_dag_dataform",
                        conf={
                            "project_id": PROJECT_ID,
                            "region": REGION,
                            "repository_id": REPOSITORY_ID,
                            "row_id": id,
                            "pipeline_name": row["cd_pipeline"],
                            "task_group": f"{domain}_task_group",
                            "id_tech_job_schedule": row["job_schedule_id"],
                            "id_tech_job_actual": f"{{{{ task_instance.xcom_pull(task_ids='{domain}_task_group.generate_id_{row['cd_pipeline']}_{id}', key='id_tech_job_actual_{row['cd_pipeline']}_{id}') }}}}",
                            "BQ_DATASET": BQ_DATASET,
                            "BQ_TABLE": BQ_TABLE,
                            "BQ_TABLE_TOTAL": BQ_TABLE_TOTAL,
                            "WEBHOOK_URL": WEBHOOK_URL,
                            "author_name": row["lb_tech_author"],
                            "cd_dag_id": DAG_ID,
                            "start_time": f"{{{{ task_instance.xcom_pull(task_ids='{domain}_task_group.create_workflow_and_capture_id_{row['cd_pipeline']}_{id}', key='start_time_{id}') }}}}",
                            "workflow_invocation_id": f"{{{{ task_instance.xcom_pull(task_ids='{domain}_task_group.create_workflow_and_capture_id_{row['cd_pipeline']}_{id}', key='workflow_invocation_{id}') }}}}",
                            "compilation_result_name": f"{{{{ task_instance.xcom_pull(task_ids='{domain}_task_group.create_compilation_result_{row['cd_pipeline']}_{id}')['name'] }}}}",
                            "compilation_result_id": f"{{{{ task_instance.xcom_pull(task_ids='{domain}_task_group.create_compilation_result_{row['cd_pipeline']}_{id}')['name'].split('/')[-1] }}}}",
                            "lb_trigger_type": row["lb_trigger_type"],
                            "lb_schedule": row["cd_pipeline"]
                        },
                        trigger_rule="all_done"
                    )

                    generate_id_task >> create_compilation_result >> invocation_workflow >> trigger_monitoring_dag