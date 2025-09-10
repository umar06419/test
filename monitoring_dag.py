from datetime import datetime
import requests
from google.cloud import bigquery
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.operators.python import get_current_context
from airflow.exceptions import AirflowException
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import ShortCircuitOperator
import pandas as pd
import logging
import json
import uuid
from google.cloud import dataform_v1beta1

default_args = {
    "owner": "rida",
    "depends_on_past": False,
    "email_on_retry": False,
    'email_on_failure': False,
    "retries": 2,
}

def proto_to_datetime(proto_ts):
    if not proto_ts or (getattr(proto_ts, "seconds", 0) == 0 and getattr(proto_ts, "nanos", 0) == 0):
        return None
    return datetime.utcfromtimestamp(proto_ts.seconds + proto_ts.nanos / 1e9)


def generate_random_id():
    return str(uuid.uuid4())

def send_webhook_notification(state, job_schedule_id, failure_reason, webhook_url, author_name, trigger_type,lb_schedule):
    """
    Sends a webhook notification based on the job state.
    """
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
                            "color": "Good" if state == "SUCCEEDED" else "Attention"
                        },
                        {
                            "type": "FactSet",
                            "facts": [
                                {"title": "State:", "value": state},
                                {"title": "Load Type:", "value": lb_schedule},
                                {"title": "Job Schedule ID:", "value": job_schedule_id},
                                {"title": "Trigger Type:", "value": trigger_type},
                                {"title": "Message:", "value": failure_reason or "No failure reason provided."},
                                {"title": "🤖 Contact Author:", "value": author_name}
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
        response = requests.post(webhook_url, json=payload)
        if response.status_code == 200:
            logging.info(f"Webhook notification sent successfully for job {job_schedule_id}.")
        else:
            logging.error(f"Failed to send webhook notification. Status code: {response.status_code}, Response: {response.text}")
    except Exception as e:
        logging.error(f"Error sending webhook notification: {e}")
        

def get_failed_actions(project_id, region, repository_id, workflow_invocation_id, state):
    client = dataform_v1beta1.DataformClient()
    invocation_path = f"projects/{project_id}/locations/{region}/repositories/{repository_id}/workflowInvocations/{workflow_invocation_id}"
    failed_actions = []
    for action in client.query_workflow_invocation_actions(request={"name": invocation_path}):
        failed_actions.append({
            "target": action.target.name if action.target else "Unknown",
            "failure_reason": action.failure_reason,
            "state": dataform_v1beta1.WorkflowInvocationAction.State(action.state).name,
            "dt_actual_start":  proto_to_datetime(action.invocation_timing.start_time),
            "dt_actual_end":  proto_to_datetime(action.invocation_timing.end_time),
        })
    return failed_actions
 
def report_status_to_bigquery(**kwargs):
    context = get_current_context()
    dag_run = context['dag_run']
    conf = dag_run.conf or {}
    ti = context['ti']
    dag = context['dag']
    project_id = conf.get('project_id')
    state = conf.get('state')
    pipeline = conf.get('pipeline_name')
    id_tech_job_schedule = conf.get('id_tech_job_schedule')
    BQ_DATASET = conf.get('BQ_DATASET')
    BQ_TABLE_TOTAL = conf.get('BQ_TABLE_TOTAL')
    author_name = conf.get('author_name', dag.owner)
    workflow_invocation_id = conf.get('workflow_invocation_id')
    compilation_result_name = conf.get('compilation_result_name')
    compilation_result_id = conf.get('compilation_result_id')
    start_time = conf.get('start_time')
    id_tech_job_actual = conf.get('id_tech_job_actual')
    # ...other conf values as needed...
    logging.info(f"In report_status_to_bigquery job schedule ID: {id_tech_job_schedule}")
    now = datetime.utcnow()
    rows_to_insert = [{
        "id_tech_job_actual": id_tech_job_actual,
        "id_tech_job_schedule": id_tech_job_schedule,
        "dt_actual_start": dag_run.start_date,
        "dt_actual_end": None,
        "lb_error": None,
        "lb_status": state,
        "fl_current": state == "RUNNING",
        "dt_tech_insert": now,
        "dt_tech_update": None,
        "lb_tech_author": author_name,
        "workflow_id": workflow_invocation_id,
        "compilation_result_id": compilation_result_id,
        "fl_inprogress": state == "RUNNING",
        "lb_dag": dag.dag_id,
        "nb_retires": ti.try_number,
        "id_tech_job_dependency": None,
        "id_tech_job_actual_dependency": None,
    }]
    df = pd.DataFrame(rows_to_insert)
    df.to_gbq(f"{BQ_DATASET}.{BQ_TABLE_TOTAL}", project_id=project_id, if_exists='append')

def get_id_tech_job_pipeline(project_id, id_tech_job_schedule, target_name):
    """
    Get id_tech_job_pipeline from BigQuery using id_tech_job_schedule and target name.
    """
    try:
        bq_client = bigquery.Client(project=project_id)
        query = f"""
        SELECT DISTINCT id_tech_job_pipeline
        FROM ds_framework_metadata.vw_job_schedule_pipeline_details 
        WHERE id_tech_job_schedule = '{id_tech_job_schedule}' 
        AND cd_pipeline = '{target_name}.sqlx'
        --cd_pipeline_group LIKE '%Level1'
        """
        result = bq_client.query(query).to_dataframe()
        
        if not result.empty:
            return result.iloc[0]['id_tech_job_pipeline']
        else:
            logging.warning(f"No pipeline ID found for schedule {id_tech_job_schedule} and target {target_name}. Using random ID.")
            return generate_random_id()
    except Exception as e:
        logging.error(f"Error retrieving pipeline ID: {e}. Using random ID.")
        return generate_random_id()

def poll_workflow_status(**kwargs):
    context = get_current_context()
    dag_run = context['dag_run']
    ti = context['ti']
    conf = dag_run.conf or {}
    project_id = conf.get('project_id')
    region = conf.get('region')
    repository_id = conf.get('repository_id')
    pipeline_name = conf.get('pipeline_name')
    id_tech_job_schedule = conf.get('id_tech_job_schedule')
    BQ_DATASET = conf.get('BQ_DATASET')
    BQ_TABLE = conf.get('BQ_TABLE')
    BQ_TABLE_TOTAL = conf.get('BQ_TABLE_TOTAL')
    author_name = conf.get('author_name')
    cd_dag_id = conf.get('cd_dag_id')
    workflow_invocation_id = conf.get('workflow_invocation_id')
    id_tech_job_actual = conf.get('id_tech_job_actual')
    webhook_url = conf.get('WEBHOOK_URL')
    row_id= conf.get('row_id')
    trigger_type = conf.get('lb_trigger_type')
    lb_schedule = conf.get('lb_schedule')

    if not workflow_invocation_id:
        raise AirflowException("Error: Workflow invocation ID missing.")

    client = dataform_v1beta1.DataformClient()
    invocation_path = f"projects/{project_id}/locations/{region}/repositories/{repository_id}/workflowInvocations/{workflow_invocation_id}"
    # id_tech_job_pipeline = generate_random_id()  # Stable ID pour toutes les updates

    while True:
        workflow_invocation = client.get_workflow_invocation(name=invocation_path)
        state = dataform_v1beta1.WorkflowInvocation.State(workflow_invocation.state).name

        if state in ["SUCCEEDED", "FAILED", "RUNNING", "SKIPPED", "PENDING"]:
            end_time = str(datetime.utcnow()) if state in ["SUCCEEDED", "FAILED"] else ""
            failed_actions = get_failed_actions(project_id, region, repository_id, workflow_invocation_id, state)
            failure_records = failed_actions if failed_actions else [{"target": pipeline_name, "state": state, "failure_reason": ""}]
            logging.info(f"Test point Workflow Invocation State: {failure_records}")
            logging.info(f"In poll_workflow_status job schedule ID: {id_tech_job_schedule}")
            data = [{
                "id_tech_job_schedule": id_tech_job_schedule,
                "id_tech_job_actual": id_tech_job_actual,
                "id_tech_job_pipeline": get_id_tech_job_pipeline(project_id, id_tech_job_schedule, failure['target']),
                "id_tech_job_pipeline_actual": generate_random_id(),
                "id_tech_source_target": failure['target'],
                "dt_actual_start": failure['dt_actual_start'],
                "dt_actual_end": failure['dt_actual_end'],
                "cd_status": failure['state'],
                "fl_inprogress": state == "RUNNING",
                "lb_error": failure['failure_reason'] if state == "FAILED" else "",
                "dt_tech_insert": None,
                "dt_tech_update": None,
                "lb_tech_author": author_name,
                "cd_dag_id": cd_dag_id,
                "am_data_size": 0,
                "partition_number": 0
            } for failure in failure_records]

            df = pd.DataFrame(data)
            df['dt_actual_start'] = pd.to_datetime(df['dt_actual_start'])
            df['dt_actual_end'] = pd.to_datetime(df['dt_actual_end'])
            df['dt_tech_insert'] = pd.Timestamp.utcnow()
            df['dt_tech_update'] = pd.Timestamp.utcnow()
            temp_table_name = f"{BQ_DATASET}.tmp_pipeline_status_{row_id}"
            schema = [
                {"name": "id_tech_job_schedule", "type": "STRING"},
                {"name": "id_tech_job_actual", "type": "STRING"},
                {"name": "id_tech_job_pipeline", "type": "STRING"},
                {"name": "id_tech_job_pipeline_actual", "type": "STRING"},
                {"name": "id_tech_source_target", "type": "STRING"},
                {"name": "dt_actual_start", "type": "TIMESTAMP"},
                {"name": "dt_actual_end", "type": "TIMESTAMP"},
                {"name": "cd_status", "type": "STRING"},
                {"name": "fl_inprogress", "type": "BOOLEAN"},
                {"name": "lb_error", "type": "STRING"},
                {"name": "dt_tech_insert", "type": "TIMESTAMP"},
                {"name": "dt_tech_update", "type": "TIMESTAMP"},
                {"name": "lb_tech_author", "type": "STRING"},
                {"name": "cd_dag_id", "type": "STRING"},
                {"name": "am_data_size", "type": "NUMERIC"},
                {"name": "partition_number", "type": "INTEGER"}
            ]
            df.to_gbq(temp_table_name, project_id=project_id, if_exists='replace', table_schema=schema)

            bq_client = bigquery.Client(project=project_id)
            merge_query = f"""
            MERGE `{project_id}.{BQ_DATASET}.{BQ_TABLE}` T
            USING (
                SELECT DISTINCT 
                    id_tech_job_schedule, 
                    id_tech_job_actual, 
                    id_tech_job_pipeline, 
                    id_tech_job_pipeline_actual, 
                    id_tech_source_target, 
                    dt_actual_start, 
                    dt_actual_end, 
                    cd_status, 
                    fl_inprogress, 
                    lb_error, 
                    dt_tech_insert, 
                    dt_tech_update, 
                    lb_tech_author, 
                    cd_dag_id, 
                    am_data_size,
                    partition_number
                FROM `{project_id}.{BQ_DATASET}.tmp_pipeline_status_{row_id}`
            ) S
            ON T.id_tech_job_actual = S.id_tech_job_actual
            AND T.id_tech_source_target = S.id_tech_source_target
            WHEN MATCHED THEN
            UPDATE SET
                dt_actual_end = S.dt_actual_end,
                cd_status = S.cd_status,
                fl_inprogress = S.fl_inprogress,
                lb_error = S.lb_error,
                dt_tech_update = CURRENT_TIMESTAMP()
            WHEN NOT MATCHED THEN
            INSERT (
                id_tech_job_schedule, 
                id_tech_job_actual, 
                id_tech_job_pipeline,
                id_tech_job_pipeline_actual, 
                id_tech_source_target, 
                dt_actual_start,
                dt_actual_end, 
                cd_status, 
                fl_inprogress, 
                lb_error, 
                dt_tech_insert,
                dt_tech_update, 
                lb_tech_author, 
                cd_dag_id, 
                am_data_size,
                partition_number
            )
            VALUES (
                S.id_tech_job_schedule, 
                S.id_tech_job_actual, 
                S.id_tech_job_pipeline,
                S.id_tech_job_pipeline_actual, 
                S.id_tech_source_target, 
                S.dt_actual_start,
                S.dt_actual_end, 
                S.cd_status, 
                S.fl_inprogress, 
                S.lb_error, 
                CURRENT_TIMESTAMP(),
                CURRENT_TIMESTAMP(), 
                S.lb_tech_author, 
                S.cd_dag_id, 
                S.am_data_size,
                S.partition_number
            );
            """
            bq_client.query(merge_query).result()

        if state in ["SUCCEEDED", "FAILED"]:
            logging.info(f"Workflow Invocation Completed: {state}")
            bq_client = bigquery.Client(project=project_id)
            table_ref = f"{BQ_DATASET}.{BQ_TABLE_TOTAL}"

            # Consolidate errors as a key-value pair: {id_tech_source_target: lb_error}
            error_dict = {}
            if not df.empty and (df['cd_status'] == 'FAILED').any():
                failed_df = df[df['cd_status'] == 'FAILED']
                error_dict = dict(zip(failed_df['id_tech_source_target'], failed_df['lb_error']))
                error_json = json.dumps(error_dict)
                lb_error_value = f"'''{error_json}'''"
            else:
                lb_error_value = "NULL"

            update_query = f"""
            UPDATE `{table_ref}`
            SET
                dt_actual_end = CURRENT_TIMESTAMP(),
                lb_status = '{state}',
                fl_current = FALSE,
                dt_tech_update = CURRENT_TIMESTAMP(),
                fl_inprogress = FALSE,
                nb_retires = {ti.try_number},
                lb_error = {lb_error_value}
            WHERE id_tech_job_actual = '{id_tech_job_actual}'
            """
            try:
                bq_client.query(update_query).result()
                logging.info(f"Row updated in BigQuery for id_tech_job_actual: {id_tech_job_actual}")
            except Exception as e:
                logging.error(f"Failed to update BigQuery: {e}")
                raise
            if state == "FAILED":
                send_webhook_notification(state, id_tech_job_schedule, f"❌ Workflow completed unsuccessfully:{lb_error_value}", webhook_url, author_name, trigger_type,lb_schedule)
            elif state == "SUCCEEDED":
                send_webhook_notification(state, id_tech_job_schedule, "✅ Workflow completed successfully.", webhook_url, author_name, trigger_type,lb_schedule)
            return

        # time.sleep(POLL_INTERVAL)

def get_dependent_if_any(**kwargs):
    context = get_current_context()
    dag_run = context['dag_run']
    conf = dag_run.conf or {}
    project_id = conf.get('project_id')
    id_tech_job_schedule = conf.get('id_tech_job_schedule')
    logging.info(f"Checking dependents for job schedule ID: {id_tech_job_schedule}")
    bq_client = bigquery.Client(project=project_id)
    dep_query = f"""
        SELECT distinct id_tech_job_schedule
        FROM ds_framework_metadata.tm_job_schedule
        WHERE lb_trigger_type = 'Dependent Only'
            AND id_tech_job_dependency = '{id_tech_job_schedule}'
    """
    dep_df = bq_client.query(dep_query).to_dataframe()
    dependent_job_schedule_ids = []
    if not dep_df.empty:
        parent_status_query = f"""
            SELECT lb_status, nb_retires
            FROM (
                SELECT lb_status, nb_retires,
                    ROW_NUMBER() OVER (
                        PARTITION BY id_tech_job_schedule 
                        ORDER BY dt_actual_start DESC
                    ) AS row_num
                FROM ds_framework_metadata.tm_job_actual
                WHERE id_tech_job_schedule = '{id_tech_job_schedule}'
            ) subquery
            WHERE row_num = 1
        """
        parent_status_df = bq_client.query(parent_status_query).to_dataframe()
        if not parent_status_df.empty and parent_status_df.iloc[0]['lb_status'] == 'SUCCEEDED':
            dependent_job_schedule_ids = dep_df['id_tech_job_schedule'].tolist()
        elif not parent_status_df.empty:
             dependent_job_schedule_ids = dep_df['id_tech_job_schedule'].tolist()
        else:
            logging.info(f"Parent job {id_tech_job_schedule} not succeeded")
    if dependent_job_schedule_ids:
        logging.info(f"Dependent job schedule IDs: {dependent_job_schedule_ids}")
        return dependent_job_schedule_ids
    else:
        logging.info(f"No dependents for job {id_tech_job_schedule}")
        return []

with DAG(
    dag_id="monitoring_dag_dataform",
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    # max_active_runs=20,
    tags=['dataform', 'reporting']
) as dag:

    report_status_task = PythonOperator(
        task_id="report_status_to_bigquery",
        python_callable=report_status_to_bigquery,
        provide_context=True,
        trigger_rule="all_done",
    )

    poll_status = PythonOperator(
        task_id="poll_workflow_status",
        python_callable=poll_workflow_status,
        provide_context=True,
        trigger_rule="all_done",
    )

    get_dependent = PythonOperator(
        task_id="get_dependent_if_any",
        python_callable=get_dependent_if_any,
        provide_context=True,
        trigger_rule="all_success",
    )

    short_circuit_dependents = ShortCircuitOperator(
        task_id="short_circuit_dependents",
        python_callable=lambda **kwargs: bool(kwargs['ti'].xcom_pull(task_ids='get_dependent_if_any')),
        provide_context=True,
        trigger_rule="all_success"
    )
    
    trigger_dependent_dag = TriggerDagRunOperator(
        task_id="trigger_dependent_dag",
        trigger_dag_id="dependent_dag_dataform",
        conf={"dependent_job_schedule_ids": "{{ ti.xcom_pull(task_ids='get_dependent_if_any') }}"},
        wait_for_completion=False,
        reset_dag_run=True,
        trigger_rule="all_success"
    )

    report_status_task >> poll_status >> get_dependent >> short_circuit_dependents >> trigger_dependent_dag