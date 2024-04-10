
WITH 

users AS (
    SELECT * FROM {{ ref('stg_users') }}
), 

fact_orders AS (
  
    SELECT * FROM {{ ref('fact_orders')}}
),

qty_orders AS (

    SELECT 
        user_id, 
        COUNT(DISTINCT order_id) AS num_orders,
        MIN(created_at) AS first_order_time,
        MAX(created_at) AS last_order_time,
        SUM(order_total) AS total_order_value
    FROM
        fact_orders
    GROUP BY 
        user_id
)

SELECT

    qty_orders.user_id,
    qty_orders.num_orders,
    qty_orders.first_order_time,
    qty_orders.last_order_time,
    qty_orders.total_order_value,
    CONCAT(users.first_name,' ', users.last_name) AS full_name,
    users.email,
    users.phone_number,
    users.created_at, 
    users.updated_at,
    users.address_id

FROM 
    qty_orders
LEFT JOIN users ON qty_orders.user_id = qty_orders.user_id