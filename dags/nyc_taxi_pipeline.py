from datetime import datetime, timedelta
import io
import pandas as pd
import pyarrow.parquet as pq
from dateutil.relativedelta import relativedelta

from airflow.sdk import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

from validation_utils import validate_bronze_layer, validate_silver_layer, validate_gold_layer
from failure_callbacks import failure_callback_function
from utils.logger import get_logger

logger = get_logger("dag_main")


DBT_PROJECT_DIR = "/opt/airflow/dbt/nyc_taxi"
CONN_ID = "postgres_airflow"
DBT_EXECUTABLE = "/home/airflow/.local/bin/dbt"

default_args = {
    'owner': 'airflow',
    'retries': 3, 
    'retry_delay': timedelta(minutes=1),
    'on_failure_callback': failure_callback_function  # <--- This handles failures globally
}

def update_pipeline_success(**context):
    run_id = context['dag_run'].run_id
    logger.info(f"🏁 Finalizing SUCCESS for Run ID: {run_id}")
    
    hook = PostgresHook(postgres_conn_id=CONN_ID)
    sql = """
        UPDATE metadata.pipeline_metadata
        SET status = 'SUCCESS',
            updated_at = CURRENT_TIMESTAMP,
            runtime_seconds = EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - created_at))
        WHERE run_id = %s;
    """
    hook.run(sql, parameters=(run_id,))

@dag(
    dag_id="yellow_taxi_full_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["bronze", "silver", "gold", "nyc_taxi", "quality_tests"]
)
def yellow_taxi_pipeline():

    pipeline_name = "yellow_taxi_monthly_load"

    create_schemas = SQLExecuteQueryOperator(
        task_id="create_schemas",
        conn_id=CONN_ID,
        sql="""
        CREATE SCHEMA IF NOT EXISTS bronze;
        CREATE SCHEMA IF NOT EXISTS silver;
        CREATE SCHEMA IF NOT EXISTS gold;
        CREATE SCHEMA IF NOT EXISTS metadata;
        """
    )

    create_bronze_table = SQLExecuteQueryOperator(
        task_id="create_bronze_table",
        conn_id=CONN_ID,
        sql="""
        CREATE TABLE IF NOT EXISTS bronze.yellow_tripdata (
            VendorID INTEGER, tpep_pickup_datetime TIMESTAMP, tpep_dropoff_datetime TIMESTAMP, passenger_count INTEGER, 
            trip_distance DOUBLE PRECISION, RatecodeID INTEGER, store_and_fwd_flag TEXT, PULocationID INTEGER, 
            DOLocationID INTEGER, payment_type INTEGER, fare_amount DOUBLE PRECISION, extra DOUBLE PRECISION, 
            mta_tax DOUBLE PRECISION, tip_amount DOUBLE PRECISION, tolls_amount DOUBLE PRECISION, 
            improvement_surcharge DOUBLE PRECISION, total_amount DOUBLE PRECISION, congestion_surcharge DOUBLE PRECISION, 
            Airport_fee DOUBLE PRECISION
        );
        """
    )

    create_metadata_table = SQLExecuteQueryOperator(
        task_id="create_metadata_table",
        conn_id=CONN_ID,
        sql="""
        CREATE TABLE IF NOT EXISTS metadata.pipeline_metadata (
            id SERIAL PRIMARY KEY, pipeline_name TEXT, run_id TEXT, load_type TEXT, target_month CHAR(7), 
            last_successful_month CHAR(7), status TEXT, runtime_seconds DOUBLE PRECISION, error_message TEXT, 
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(run_id)
        );
        """
    )

   
    @task
    def load_yellow_taxi_incremental(**context):
        run_id = context['dag_run'].run_id
        hook = PostgresHook(postgres_conn_id=CONN_ID)
        conn = hook.get_conn()
        cur = conn.cursor()

        try:
          
            cur.execute("SELECT MAX(target_month) FROM metadata.pipeline_metadata WHERE pipeline_name=%s AND status='SUCCESS';", (pipeline_name,))
            row = cur.fetchone()
            last_successful_month = row[0] if row and row[0] else None
            
            if last_successful_month:
                next_month_dt = datetime.strptime(last_successful_month, "%Y-%m") + relativedelta(months=1)
            else:
                next_month_dt = datetime.strptime("2024-01", "%Y-%m")
            
            target_month = next_month_dt.strftime("%Y-%m")
            file_path = f"/opt/airflow/data/yellow_tripdata_{target_month}.parquet"
            
            logger.info(f"📥 Loading target month: {target_month} from {file_path}")


            cur.execute("""
                INSERT INTO metadata.pipeline_metadata (pipeline_name, run_id, load_type, target_month, last_successful_month, status) 
                VALUES (%s, %s, %s, %s, %s, 'RUNNING')
                ON CONFLICT (run_id) DO NOTHING;
            """, (pipeline_name, run_id, "INCREMENTAL", target_month, last_successful_month))
            conn.commit()

          
            parquet_file = pq.ParquetFile(file_path)
            int_cols = ["vendorid", "passenger_count", "ratecodeid", "pulocationid", "dolocationid", "payment_type"]
            
           
            cur.execute("DELETE FROM bronze.yellow_tripdata WHERE TO_CHAR(tpep_pickup_datetime, 'YYYY-MM') = %s;", (target_month,))
            
            batch_count = 0
            for batch in parquet_file.iter_batches(batch_size=100_000):
                df = batch.to_pandas()
                df.columns = [c.lower() for c in df.columns]
                
                
                df["month"] = df["tpep_pickup_datetime"].dt.strftime("%Y-%m")
                df = df[df["month"] == target_month].drop(columns=["month"])
                
                if df.empty: continue
                
               
                for col in int_cols:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors='coerce').astype("Int64")

              
                buffer = io.StringIO()
                df.to_csv(buffer, index=False, header=False, na_rep='\\N')
                buffer.seek(0)
                cur.copy_expert("COPY bronze.yellow_tripdata FROM STDIN WITH CSV NULL '\\N';", buffer)
                batch_count += 1
            
            logger.info(f"✅ Successfully loaded {batch_count} batches for {target_month}")
            conn.commit()

        except Exception as e:
            logger.error(f"❌ Bronze Load Failed: {e}")
            raise e
        finally:
            cur.close()
            conn.close()

    load_bronze_task = load_yellow_taxi_incremental()

   
    validate_bronze_task = PythonOperator(task_id="validate_bronze_quality", python_callable=validate_bronze_layer)
    validate_silver_task = PythonOperator(task_id="validate_silver_quality", python_callable=validate_silver_layer)
    validate_gold_task = PythonOperator(task_id="validate_gold_quality", python_callable=validate_gold_layer)


    dbt_env = {"DBT_PROFILES_DIR": DBT_PROJECT_DIR, "PATH": "/home/airflow/.local/bin:/usr/local/bin:/usr/bin:/bin"}
    
    dbt_silver_run = BashOperator(task_id="dbt_silver_run", bash_command=f"cd {DBT_PROJECT_DIR} && {DBT_EXECUTABLE} run --select silver", env=dbt_env)
    dbt_silver_test = BashOperator(task_id="dbt_silver_test", bash_command=f"cd {DBT_PROJECT_DIR} && {DBT_EXECUTABLE} test --select silver", env=dbt_env)
    
    dbt_gold_run = BashOperator(task_id="dbt_gold_run", bash_command=f"cd {DBT_PROJECT_DIR} && {DBT_EXECUTABLE} run --select gold", env=dbt_env)
    dbt_gold_test = BashOperator(task_id="dbt_gold_test", bash_command=f"cd {DBT_PROJECT_DIR} && {DBT_EXECUTABLE} test --select gold", env=dbt_env)

    
    finalize_success = PythonOperator(
        task_id="finalize_metadata_success", 
        python_callable=update_pipeline_success, 
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

    create_schemas >> [create_bronze_table, create_metadata_table] >> load_bronze_task
    
    load_bronze_task >> validate_bronze_task
    validate_bronze_task >> dbt_silver_run >> dbt_silver_test >> validate_silver_task
    validate_silver_task >> dbt_gold_run >> dbt_gold_test >> validate_gold_task
    
    validate_gold_task >> finalize_success

yellow_taxi_pipeline()