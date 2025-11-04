{{ config(schema='marts', alias='customers_order_details') }}

WITH 
customers AS (
	SELECT * FROM main_staging.stg_customers sc 
	)
, customers_orders_detail AS (
	SELECT DISTINCT
		  so.customer_id 
		, COUNT( * ) OVER ( PARTITION BY so.customer_id ) AS total_orders
		, MIN( order_purchase_timestamp ) OVER ( PARTITION BY so.customer_id ) AS first_purchase
		, MIN( order_delivered_customer_date ) OVER ( PARTITION BY so.customer_id ) AS first_delivery
 	FROM main_staging.stg_orders so 
	)
SELECT 
	  ctm.customer_id
	, cod.total_orders
	, cod.first_purchase
	, cod.first_delivery
FROM customers AS ctm
LEFT JOIN customers_orders_detail AS cod ON ctm.customer_id = cod.customer_id