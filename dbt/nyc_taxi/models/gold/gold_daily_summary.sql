{{ config(
    materialized='incremental',
    unique_key='trip_date'
) }}

SELECT
    DATE(tpep_pickup_datetime) AS trip_date,
    COUNT(*) AS total_trips,
    SUM(passenger_count) AS total_passengers,
    SUM(trip_distance) AS total_distance_miles,
    SUM(total_amount) AS total_revenue,
    SUM(tip_amount) AS total_tips,
    AVG(fare_amount) AS avg_fare,
    AVG(trip_distance) AS avg_trip_distance
FROM {{ ref('silver_yellow_tripdata') }}

{% if is_incremental() %}
WHERE tpep_pickup_datetime > (SELECT MAX(trip_date) FROM {{ this }})
{% endif %}

GROUP BY 1