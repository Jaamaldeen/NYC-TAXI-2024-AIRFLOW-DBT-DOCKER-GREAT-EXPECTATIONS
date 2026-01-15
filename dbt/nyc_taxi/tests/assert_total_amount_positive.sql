SELECT *
FROM {{ ref('silver_yellow_tripdata') }}
WHERE total_amount < 0