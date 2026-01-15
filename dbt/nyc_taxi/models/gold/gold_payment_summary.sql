{{ config(materialized='table') }}

SELECT
    COALESCE(payment_description, 'Unknown') AS payment_description,
    COUNT(*) AS trip_count,
    SUM(total_amount) AS total_revenue,
    SUM(tip_amount) AS total_tips,
    CASE 
        WHEN SUM(total_amount) <= 0 THEN 0
        ELSE (SUM(tip_amount) / SUM(total_amount)) * 100 
    END AS avg_tip_percent
FROM {{ ref('silver_yellow_tripdata') }}
GROUP BY 1