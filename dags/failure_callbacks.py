import json
import urllib.request # <--- Standard library, no pip install needed
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from utils.logger import get_logger

logger = get_logger("failure_callback")

def failure_callback_function(context):
    dag_run = context.get('dag_run')
    task_instance = context.get('task_instance')
    run_id = dag_run.run_id
    task_id = task_instance.task_id
    execution_date = context.get('execution_date')
    exception = context.get('exception')
    log_url = task_instance.log_url
    
    error_summary = str(exception)[:500]

    logger.error(f"PIPELINE FAILURE detected in task '{task_id}'")


    try:
        hook = PostgresHook(postgres_conn_id="postgres_airflow")
        sql = "UPDATE metadata.pipeline_metadata SET status = 'FAILED', error_message = %s, updated_at = CURRENT_TIMESTAMP WHERE run_id = %s;"
        hook.run(sql, parameters=(error_summary, run_id))
    except Exception as e:
        logger.error(f"DB Update Failed: {e}")

    
    try:
        webhook_url = Variable.get("slack_webhook_url", default_var=None)
        
        if not webhook_url:
            logger.error("ERROR: 'slack_webhook_url' variable is missing!")
            return

        slack_data = {
            "text": f":red_circle: *Task Failed*\n*Task*: {task_id}\n*Dag*: {dag_run.dag_id}\n*Error*: `{error_summary}`"
        }
        
        req = urllib.request.Request(
            webhook_url, 
            data=json.dumps(slack_data).encode('utf-8'), 
            headers={'Content-Type': 'application/json'}
        )
        
        with urllib.request.urlopen(req) as response:
            logger.info(f"Slack sent. Status: {response.getcode()}")
            
    except Exception as e:
        logger.error(f"Slack Failed: {e}")