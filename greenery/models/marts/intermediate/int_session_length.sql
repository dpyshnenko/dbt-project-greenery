WITH 

events_data AS (
    SELECT * FROM {{ ref('stg_events')}}
)


SELECT 
    session_id,
    min(created_at) AS start_session,
    max(created_at) AS end_session,
    DATEDIFF('hour', MIN(created_at), MAX(created_at)) AS session_duration_hours
FROM 
    events_data
GROUP BY 
    session_id
