WITH 

orders AS (
	SELECT * FROM {{ ref('stg_orders') }}
),

addresses AS (
    SELECT * FROM {{ ref('stg_addresses')}}
),

promos AS (
    SELECT * FROM {{ ref('stg_promos')}}
),
 
order_items AS (
    SELECT * FROM {{ ref('stg_order_items')}}
),

number_produtcs AS (
SELECT 
    order_id,
    COUNT(DISTINCT product_id) AS num_products,
    SUM(quantity) AS total_quantity
FROM 
    order_items
GROUP BY 
    order_id
)


SELECT 
    orders.order_id, 
    orders.user_id, 
    orders.promo_id, 
    promos.discount,
    promos.promo_status,
    orders.address_id,
    addresses.address,
    addresses.zipcode,
    addresses.state,
    addresses.country,
    orders.created_at, 
    orders.order_cost,
    orders.shipping_cost,
    orders.order_total, 
    orders.tracking_id, 
    orders.shipping_service,
    orders.estimated_delivery_at,
    orders.delivered_at,
    orders.status,
    number_produtcs.num_products,
    number_produtcs.total_quantity
FROM   
    orders
LEFT JOIN  addresses 
    ON orders.address_id = addresses.address_id
LEFT JOIN promos
    ON orders.promo_id = promos.promo_id
LEFT JOIN number_produtcs
    ON orders.order_id = number_produtcs.order_id