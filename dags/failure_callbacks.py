from airflow.providers.postgres.hooks.postgres import PostgresHook
from utils.logger import get_logger

logger = get_logger("failure_callback")

def failure_callback_function(context):
    run_id = context['dag_run'].run_id
    task_id = context['task_instance'].task_id
    exception = context.get('exception')

    logger.error(f"🚨 PIPELINE FAILURE detected in task '{task_id}' for Run ID: {run_id}")
    logger.error(f"Error Details: {exception}")

    try:
        hook = PostgresHook(postgres_conn_id="postgres_airflow")
        sql = """
            UPDATE metadata.pipeline_metadata
            SET status = 'FAILED',
                error_message = %s,
                updated_at = CURRENT_TIMESTAMP
            WHERE run_id = %s;
        """
        error_msg = str(exception)[:500]
        hook.run(sql, parameters=(error_msg, run_id))
        logger.info("✅ Metadata marked as FAILED in database.")
    except Exception as e:
        logger.error(f"❌ Failed to update metadata on failure: {e}")