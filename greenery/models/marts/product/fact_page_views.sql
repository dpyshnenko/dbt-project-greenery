WITH 

products AS (
    SELECT * FROM {{ ref('stg_products')}}
),

session_duration AS (
    SELECT * FROM {{ ref('int_session_length')}}
),

users AS (
    SELECT * FROM {{ ref('stg_users')}}
),

addresses AS (
    SELECT * FROM {{ ref('stg_addresses')}}
)

{% set event_types = dbt_utils.get_column_values(
    table=ref('stg_events'), 
    column='event_type'
) %}

SELECT 
    events.session_id, 
    events.user_id,
    session_duration.start_session,
    session_duration.end_session,
    session_duration.session_duration_hours,
    users.address_id,
    addresses.address, 
    addresses.state,
    addresses.country,
    COUNT(DISTINCT events.order_id) AS qty_orders,
    COUNT(DISTINCT product_id) AS qty_products,
    SUM(CASE WHEN events.event_type = ' ' THEN 1 ELSE 0 END) AS is_checkout,
    SUM(CASE WHEN events.event_type = 'package_shipped' THEN 1 ELSE 0 END) AS is_shipped,
    SUM(CASE WHEN events.event_type = 'add_to_cart' THEN 1 ELSE 0 END) AS is_added,
    SUM(CASE WHEN events.event_type = 'page_view' THEN 1 ELSE 0 END) AS is_viewed
FROM 
    DEV_DB.DBT_DVPYSHNENKOGMAILCOM.STG_EVENTS events
LEFT JOIN session_duration
    ON events.session_id = session_duration.session_id
LEFT JOIN users
    ON events.user_id = users.user_id
LEFT JOIN addresses
    ON users.address_id = addresses.address_id
GROUP BY 
    events.session_id, 
    events.user_id,
    session_duration.start_session,
    session_duration.end_session,
    session_duration.session_duration_hours,
    users.address_id,
    addresses.address, 
    addresses.state,
    addresses.country
    
