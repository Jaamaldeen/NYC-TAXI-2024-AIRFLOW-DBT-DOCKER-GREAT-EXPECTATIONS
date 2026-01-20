{{ config(
    materialized='incremental',
    unique_key='unique_trip_id',
    incremental_strategy='delete+insert'
) }}

WITH source_data AS (
    SELECT * FROM {{ ref('bronze_yellow_tripdata') }}
    {% if is_incremental() %}
    WHERE tpep_pickup_datetime > (SELECT MAX(tpep_pickup_datetime) FROM {{ this }})
    {% endif %}
),

transformed AS (
    SELECT
        vendorid, ratecodeid, payment_type, pulocationid, dolocationid,
        store_and_fwd_flag, passenger_count, trip_distance,
        tpep_pickup_datetime, tpep_dropoff_datetime,
        
        CASE vendorid 
            WHEN 1 THEN 'Creative Mobile Technologies, LLC' 
            WHEN 2 THEN 'Curb Mobility, LLC' 
            WHEN 6 THEN 'Myle Technologies Inc' 
            WHEN 7 THEN 'Helix' 
            ELSE 'Unknown' 
        END AS vendor_name,

        CASE ratecodeid 
            WHEN 1 THEN 'Standard rate' 
            WHEN 2 THEN 'JFK' 
            WHEN 3 THEN 'Newark' 
            WHEN 4 THEN 'Nassau or Westchester' 
            WHEN 5 THEN 'Negotiated fare' 
            WHEN 6 THEN 'Group ride' 
            ELSE 'Null/unknown' 
        END AS rate_description,

        CASE payment_type 
            WHEN 0 THEN 'Flex Fare trip' 
            WHEN 1 THEN 'Credit card' 
            WHEN 2 THEN 'Cash' 
            WHEN 3 THEN 'No charge' 
            WHEN 4 THEN 'Dispute' 
            WHEN 5 THEN 'Unknown' 
            WHEN 6 THEN 'Voided trip' 
            ELSE 'Unknown' 
        END AS payment_description,

       
        ROUND(CAST(EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime)) / 60 AS numeric), 2) AS trip_duration_minutes,

        ABS(COALESCE(fare_amount, 0)) AS fare_amount,
        ABS(COALESCE(extra, 0)) AS extra,
        ABS(COALESCE(mta_tax, 0)) AS mta_tax,
        ABS(COALESCE(tip_amount, 0)) AS tip_amount,
        ABS(COALESCE(tolls_amount, 0)) AS tolls_amount,
        ABS(COALESCE(improvement_surcharge, 0)) AS improvement_surcharge,
        ABS(COALESCE(congestion_surcharge, 0)) AS congestion_surcharge,
        ABS(COALESCE(airport_fee, 0)) AS airport_fee

    FROM source_data
    
    WHERE payment_type IN (1, 2, 3, 4, 5, 6)
),

final_calculations AS (
    SELECT 
        *,
        (fare_amount + extra + mta_tax + tip_amount + tolls_amount + improvement_surcharge + congestion_surcharge + airport_fee) AS total_amount,

       
        md5(
            COALESCE(CAST(vendorid AS VARCHAR), '-1') || 
            COALESCE(CAST(tpep_pickup_datetime AS VARCHAR), '1900-01-01') || 
            COALESCE(CAST(tpep_dropoff_datetime AS VARCHAR), '1900-01-01') || 
            COALESCE(CAST(pulocationid AS VARCHAR), '-1') || 
            COALESCE(CAST(dolocationid AS VARCHAR), '-1') || 
            COALESCE(CAST(passenger_count AS VARCHAR), '0') || 
            COALESCE(CAST(trip_distance AS VARCHAR), '0')
        ) AS unique_trip_id
    FROM transformed
)


SELECT DISTINCT ON (unique_trip_id) *
FROM final_calculations
ORDER BY unique_trip_id, tpep_pickup_datetime


