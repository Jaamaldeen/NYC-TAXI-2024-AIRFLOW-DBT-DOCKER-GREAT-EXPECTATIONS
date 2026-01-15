{{ config(materialized='table') }}

SELECT
    vendor_name,
    COUNT(*) AS total_trips,
    SUM(total_amount) AS total_revenue,
    SUM(trip_distance) AS total_distance,
    AVG(trip_distance) AS avg_trip_distance,
    AVG(fare_amount) AS avg_fare
FROM {{ ref('silver_yellow_tripdata') }}
GROUP BY 1