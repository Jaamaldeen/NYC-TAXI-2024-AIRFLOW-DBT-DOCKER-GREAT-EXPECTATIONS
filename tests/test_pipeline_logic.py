import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime
from dateutil.relativedelta import relativedelta
from dags.failure_callbacks import failure_callback_function
from dags.nyc_taxi_pipeline import update_pipeline_success, yellow_taxi_pipeline


dag = yellow_taxi_pipeline()
load_task_function = dag.get_task("load_yellow_taxi_staging").python_callable


class TestMonthCalculation:
    
    @patch("dags.nyc_taxi_pipeline.PostgresHook")
    def test_calculate_next_month_initial_run(self, mock_hook_cls):
        mock_hook = mock_hook_cls.return_value
        mock_conn = mock_hook.get_conn.return_value
        mock_cursor = mock_conn.cursor.return_value
        
        mock_cursor.fetchone.return_value = None
        mock_context = {
            'dag_run': MagicMock(run_id='test_run_1'),
            'ti': MagicMock()
        }

        with patch("dags.nyc_taxi_pipeline.pq.ParquetFile") as mock_pq:
            mock_pq.return_value.iter_batches.return_value = [] 
            
            load_task_function(**mock_context)
            
            args, _ = mock_cursor.execute.call_args_list[1]
            assert "2024-01" in args[0] or "2024-01" in str(args)

    @patch("dags.nyc_taxi_pipeline.PostgresHook")
    def test_calculate_next_month_incremental(self, mock_hook_cls):
        mock_cursor = mock_hook_cls.return_value.get_conn.return_value.cursor.return_value
 
        mock_cursor.fetchone.return_value = ['2024-05']

        mock_context = {'dag_run': MagicMock(run_id='test_run_2')}

        with patch("dags.nyc_taxi_pipeline.pq.ParquetFile") as mock_pq:
            mock_pq.return_value.iter_batches.return_value = []
            
            load_task_function(**mock_context)
            
            insert_call = mock_cursor.execute.call_args_list[1]
            sql_params = insert_call[0][1] 
            
            assert sql_params[3] == "2024-06"
            assert sql_params[4] == "2024-05"


class TestMetadataUpdates:

    @patch("dags.nyc_taxi_pipeline.PostgresHook")
    def test_update_pipeline_success(self, mock_hook_cls):
        mock_hook = mock_hook_cls.return_value
        
        context = {'dag_run': MagicMock(run_id='manual__123')}
        
        update_pipeline_success(**context)

        called_sql = mock_hook.run.call_args[0][0]
        called_params = mock_hook.run.call_args[1]['parameters']
        
        assert "SET status = 'SUCCESS'" in called_sql
        assert called_params == ('manual__123',)


class TestFailureCallback:

    @patch("dags.failure_callbacks.PostgresHook")
    def test_failure_callback_writes_error(self, mock_hook_cls):
    
        mock_hook = mock_hook_cls.return_value
        
        fake_error = ValueError("Validation crashed because of nulls!")
        
        context = {
            'dag_run': MagicMock(run_id='fail_run_999'),
            'task_instance': MagicMock(task_id='validate_bronze'),
            'exception': fake_error
        }
        
        failure_callback_function(context)
        
        called_sql = mock_hook.run.call_args[0][0]
        called_params = mock_hook.run.call_args[1]['parameters']
        
        assert "SET status = 'FAILED'" in called_sql
    
        assert "Validation crashed because of nulls!" in called_params[0]
        assert called_params[1] == 'fail_run_999'


class TestIdempotency:

    @patch("dags.nyc_taxi_pipeline.PostgresHook")
    def test_idempotency_delete_before_insert(self, mock_hook_cls):
        mock_cursor = mock_hook_cls.return_value.get_conn.return_value.cursor.return_value
        
        mock_cursor.fetchone.return_value = ['2024-01'] 
        
        mock_context = {'dag_run': MagicMock(run_id='idempotency_test')}

        with patch("dags.nyc_taxi_pipeline.pq.ParquetFile") as mock_pq:
            mock_pq.return_value.iter_batches.return_value = []
            
            load_task_function(**mock_context)
            
            delete_called = False
            for call in mock_cursor.execute.call_args_list:
                sql = call[0][0]
                if "DELETE FROM bronze.bronze_yellow_tripdata" in sql:
                    delete_called = True
                    assert call[0][1] == ('2024-02',)
            
            assert delete_called, "The code must execute a DELETE statement to ensure idempotency"