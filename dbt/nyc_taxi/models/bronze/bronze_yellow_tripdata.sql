{{ config(
    materialized='incremental',
    schema='bronze',
    unique_key=['vendorid', 'tpep_pickup_datetime'] 
) }}

WITH source_data AS (
    SELECT
        VendorID, 
        tpep_pickup_datetime, 
        tpep_dropoff_datetime, 
        passenger_count, 
        trip_distance, 
        RatecodeID, 
        store_and_fwd_flag, 
        PULocationID, 
        DOLocationID, 
        payment_type, 
        fare_amount, 
        extra, 
        mta_tax, 
        tip_amount, 
        tolls_amount, 
        improvement_surcharge, 
        total_amount, 
        congestion_surcharge, 
        Airport_fee
    FROM {{ source('staging', 'yellow_tripdata_raw') }}
)

SELECT * FROM source_data

{% if is_incremental() %}
  WHERE TO_CHAR(tpep_pickup_datetime, 'YYYY-MM') = '{{ var("target_month") }}'
{% endif %}