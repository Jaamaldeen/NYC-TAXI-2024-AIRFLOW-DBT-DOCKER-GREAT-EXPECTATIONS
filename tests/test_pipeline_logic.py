import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime
from dateutil.relativedelta import relativedelta

# Import the code we want to test
# Note: In a real setup, ensure your PYTHONPATH includes 'dags'
from dags.failure_callbacks import failure_callback_function
from dags.nyc_taxi_pipeline import update_pipeline_success, yellow_taxi_pipeline

# We need to extract the raw python function from the @task decorator for testing
# This assumes 'yellow_taxi_pipeline' DAG is parsed and we can access tasks
dag = yellow_taxi_pipeline()
load_task_function = dag.get_task("load_yellow_taxi_incremental").python_callable

# --- TEST 1: Month Calculation Logic (Metadata -> Next Month) ---
class TestMonthCalculation:
    
    @patch("dags.nyc_taxi_pipeline.PostgresHook")
    def test_calculate_next_month_initial_run(self, mock_hook_cls):
        """Test Case A: No history in DB, should default to 2024-01"""
        # Setup Mock DB
        mock_hook = mock_hook_cls.return_value
        mock_conn = mock_hook.get_conn.return_value
        mock_cursor = mock_conn.cursor.return_value
        
        # Mock fetchone returning None (Empty Table)
        mock_cursor.fetchone.return_value = None

        # Prepare Context
        mock_context = {
            'dag_run': MagicMock(run_id='test_run_1'),
            'ti': MagicMock()
        }

        # Run the task logic (Mocking ParquetFile to avoid actual file I/O)
        with patch("dags.nyc_taxi_pipeline.pq.ParquetFile") as mock_pq:
            mock_pq.return_value.iter_batches.return_value = [] # Empty iterator
            
            # Execute
            load_task_function(**mock_context)
            
            # Assertions
            # Check if it tried to load 2024-01
            args, _ = mock_cursor.execute.call_args_list[1] # Index 1 is the INSERT metadata
            assert "2024-01" in args[0] or "2024-01" in str(args)

    @patch("dags.nyc_taxi_pipeline.PostgresHook")
    def test_calculate_next_month_incremental(self, mock_hook_cls):
        """Test Case B: History exists (2024-05), should return 2024-06"""
        mock_cursor = mock_hook_cls.return_value.get_conn.return_value.cursor.return_value
        
        # Mock fetchone returning '2024-05'
        mock_cursor.fetchone.return_value = ['2024-05']

        mock_context = {'dag_run': MagicMock(run_id='test_run_2')}

        with patch("dags.nyc_taxi_pipeline.pq.ParquetFile") as mock_pq:
            mock_pq.return_value.iter_batches.return_value = []
            
            load_task_function(**mock_context)
            
            # Verify the INSERT statement used the calculated month
            insert_call = mock_cursor.execute.call_args_list[1]
            # Args are usually passed as a tuple in the second argument of execute
            sql_params = insert_call[0][1] 
            
            # Params: (pipeline_name, run_id, type, TARGET_MONTH, last_month, ...)
            assert sql_params[3] == "2024-06"
            assert sql_params[4] == "2024-05"


# --- TEST 2: Metadata Update Functions (RUNNING/SUCCESS) ---
class TestMetadataUpdates:

    @patch("dags.nyc_taxi_pipeline.PostgresHook")
    def test_update_pipeline_success(self, mock_hook_cls):
        """Test that success function updates status to 'SUCCESS'"""
        mock_hook = mock_hook_cls.return_value
        
        context = {'dag_run': MagicMock(run_id='manual__123')}
        
        # Run function
        update_pipeline_success(**context)
        
        # Verify SQL
        called_sql = mock_hook.run.call_args[0][0]
        called_params = mock_hook.run.call_args[1]['parameters']
        
        assert "SET status = 'SUCCESS'" in called_sql
        assert called_params == ('manual__123',)


# --- TEST 3: Failure Callback Logic ---
class TestFailureCallback:

    @patch("dags.failure_callbacks.PostgresHook")
    def test_failure_callback_writes_error(self, mock_hook_cls):
        """Test that callback captures exception and writes FAILED status"""
        mock_hook = mock_hook_cls.return_value
        
        # Create a Fake Exception
        fake_error = ValueError("Validation crashed because of nulls!")
        
        context = {
            'dag_run': MagicMock(run_id='fail_run_999'),
            'task_instance': MagicMock(task_id='validate_bronze'),
            'exception': fake_error
        }
        
        # Run Callback
        failure_callback_function(context)
        
        # Verify SQL execution
        called_sql = mock_hook.run.call_args[0][0]
        called_params = mock_hook.run.call_args[1]['parameters']
        
        assert "SET status = 'FAILED'" in called_sql
        # Check if error message is passed to DB
        assert "Validation crashed because of nulls!" in called_params[0]
        assert called_params[1] == 'fail_run_999'


# --- TEST 4: Idempotency Logic (Dedupe/Merge) ---
class TestIdempotency:

    @patch("dags.nyc_taxi_pipeline.PostgresHook")
    def test_idempotency_delete_before_insert(self, mock_hook_cls):
        """
        Verify that we DELETE existing data for the target month 
        before inserting new data (Idempotency strategy).
        """
        mock_cursor = mock_hook_cls.return_value.get_conn.return_value.cursor.return_value
        
        # Setup: Target month is 2024-02
        mock_cursor.fetchone.return_value = ['2024-01'] 
        
        mock_context = {'dag_run': MagicMock(run_id='idempotency_test')}

        with patch("dags.nyc_taxi_pipeline.pq.ParquetFile") as mock_pq:
            mock_pq.return_value.iter_batches.return_value = []
            
            # Run Task
            load_task_function(**mock_context)
            
            # Inspect all SQL calls
            # We expect a DELETE call with the target month
            delete_called = False
            for call in mock_cursor.execute.call_args_list:
                sql = call[0][0]
                if "DELETE FROM bronze.yellow_tripdata" in sql:
                    delete_called = True
                    # Check params for correct month
                    assert call[0][1] == ('2024-02',)
            
            assert delete_called, "The code must execute a DELETE statement to ensure idempotency"