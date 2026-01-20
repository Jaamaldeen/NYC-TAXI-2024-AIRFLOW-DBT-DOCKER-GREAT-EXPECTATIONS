{{ config(
    materialized='incremental',
    unique_key='revenue_month'
) }}

WITH silver_data AS (
    SELECT * FROM {{ ref('silver_yellow_tripdata') }}
)

SELECT
    date_trunc('month', tpep_pickup_datetime) as revenue_month,
    sum(total_amount) as total_monthly_revenue,
    sum(passenger_count) as total_monthly_passengers,
    count(*) as total_monthly_trips,
    avg(trip_distance) as avg_trip_distance
FROM silver_data

{% if is_incremental() %}
  WHERE date_trunc('month', tpep_pickup_datetime) >= (
      SELECT COALESCE(MAX(revenue_month), '1900-01-01'::timestamp) FROM {{ this }}
  )
{% endif %}

GROUP BY 1