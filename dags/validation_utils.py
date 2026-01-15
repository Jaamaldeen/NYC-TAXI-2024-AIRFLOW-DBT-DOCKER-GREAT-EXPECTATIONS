import great_expectations as gx
import great_expectations.expectations as gxe
from great_expectations.exceptions import DataContextError  # <--- Essential Import
from airflow.providers.postgres.hooks.postgres import PostgresHook
from utils.logger import get_logger

# Use our custom logger
logger = get_logger("validation_utils")

def get_connection_string(conn_id="postgres_airflow"):
    """
    Retrieves connection details securely from Airflow Connection.
    """
    hook = PostgresHook(postgres_conn_id=conn_id)
    conn = hook.get_connection(conn_id)
    return f"postgresql+psycopg2://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}"

def run_validation(layer_name, table_name, suite_name, expectations):
    """
    Generic validation runner for GX 1.0 (Fluent API).
    """
    logger.info(f"🚦 STARTING: {layer_name} Validation on table {table_name}...")

    try:
        context = gx.get_context()
        datasource_name = f"postgres_{layer_name}_source"
        connection_string = get_connection_string()

        # 1. Get or Create Datasource
        try:
            datasource = context.data_sources.get(datasource_name)
        except (KeyError, ValueError, DataContextError):
            datasource = context.data_sources.add_postgres(
                name=datasource_name, 
                connection_string=connection_string
            )

        # 2. Get or Create Asset (The Table)
        asset_name = f"{layer_name}_{table_name}_asset"
        existing_asset = next((a for a in datasource.assets if a.name == asset_name), None)
        
        if existing_asset:
            data_asset = existing_asset
        else:
            data_asset = datasource.add_table_asset(
                name=asset_name, 
                table_name=table_name, 
                schema_name=layer_name.split('_')[0] if '_' in layer_name else layer_name
            )

        # 3. Get or Create Batch Definition
        batch_def_name = f"{layer_name}_whole_table"
        try:
            batch_definition = data_asset.get_batch_definition(batch_def_name)
        except (KeyError, ValueError, DataContextError):
            batch_definition = data_asset.add_batch_definition(name=batch_def_name)

        # 4. Get or Create Expectation Suite
        try:
            # Try to get existing suite
            suite = context.suites.get(suite_name)
            # Clear old expectations to avoid duplicates on re-runs
            suite.expectations = [] 
        except (KeyError, ValueError, DataContextError): # <--- FIXED: Now catches DataContextError
            logger.info(f"Suite '{suite_name}' not found. Creating it...")
            suite = context.suites.add(gx.ExpectationSuite(name=suite_name))

        # 5. Add Expectations dynamically
        for exp in expectations:
            suite.add_expectation(exp)

        # 6. Run Validation
        validation_def_name = f"{layer_name}_validator"
        try:
            validation_def = context.validation_definitions.get(validation_def_name)
        except (KeyError, ValueError, DataContextError):
            validation_def = context.validation_definitions.add(
                gx.ValidationDefinition(name=validation_def_name, data=batch_definition, suite=suite)
            )

        results = validation_def.run()

        # 7. Check Results
        if not results.success:
            failed_expectations = []
            for res in results.results:
                if not res.success:
                    failed_expectations.append({
                        "expectation": res.expectation_config.type,
                        "column": res.expectation_config.kwargs.get("column"),
                        "unexpected_percent": res.result.get("unexpected_percent")
                    })
            
            error_msg = f"❌ {layer_name} Check Failed! Failures: {failed_expectations}"
            logger.error(error_msg)
            raise ValueError(error_msg)

        logger.info(f"✅ {layer_name} Validation Passed Successfully!")

    except Exception as e:
        logger.exception(f"CRITICAL: Validation script crashed for {layer_name}")
        raise e

# --- Wrapper Functions called by DAG ---

def validate_bronze_layer(**kwargs):
    expected_cols = [
        "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count", 
        "trip_distance", "RatecodeID", "store_and_fwd_flag", "PULocationID", 
        "DOLocationID", "payment_type", "fare_amount", "extra", "mta_tax", 
        "tip_amount", "tolls_amount", "improvement_surcharge", "total_amount", 
        "congestion_surcharge"
    ]
    
    expectations = [gxe.ExpectTableRowCountToBeBetween(min_value=1)]
    for col in expected_cols:
        expectations.append(gxe.ExpectColumnToExist(column=col))
    
    expectations.append(gxe.ExpectColumnValuesToNotBeNull(column="tpep_pickup_datetime", mostly=0.99))

    run_validation("bronze", "yellow_tripdata", "bronze_schema_suite", expectations)

def validate_silver_layer(**kwargs):
    expectations = [
        gxe.ExpectColumnValuesToNotBeNull(column="vendorid", mostly=1.0),
        gxe.ExpectColumnValuesToNotBeNull(column="tpep_pickup_datetime", mostly=1.0),
        gxe.ExpectColumnValuesToBeBetween(column="total_amount", min_value=0, mostly=0.99),
        gxe.ExpectColumnValuesToBeBetween(column="trip_distance", min_value=0, mostly=0.99),
        gxe.ExpectColumnValuesToBeInSet(column="payment_type", value_set=[1, 2, 3, 4, 5, 6], mostly=0.99)
    ]
    run_validation("silver", "silver_yellow_tripdata", "silver_business_rules", expectations)

def validate_gold_layer(**kwargs):
    expectations = [
        gxe.ExpectColumnValuesToBeBetween(column="total_monthly_revenue", min_value=0, max_value=1000000000),
        gxe.ExpectColumnValuesToBeBetween(column="total_monthly_trips", min_value=1, max_value=10000000),
        gxe.ExpectColumnValuesToNotBeNull(column="revenue_month")
    ]
    run_validation("gold", "gold_monthly_summary", "gold_reporting_suite", expectations)