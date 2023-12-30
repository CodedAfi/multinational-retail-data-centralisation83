-- Query to identify the count and distribution of stores across countries


SELECT country_code, COUNT(*) AS store_count
FROM dim_store_details
GROUP BY country_code
ORDER BY store_count DESC;

-- Query to determine top locations based on store count


SELECT locality, COUNT(*) AS store_count
FROM dim_store_details
GROUP BY locality
ORDER BY store_count DESC
LIMIT 20;

-- Analysing the months with the highest average sales cost


SELECT dim_date_times.month, SUM(dim_products.product_price * product_quantity) AS monthly_sales
FROM orders_table
JOIN dim_date_times ON orders_table.date_uuid = dim_date_times.date_uuid
JOIN dim_products ON orders_table.product_code = dim_products.product_code
GROUP BY dim_date_times.month
ORDER BY monthly_sales DESC;

-- Assessing online versus offline sales

SELECT 'Web' AS sale_type
FROM dim_store_details
WHERE store_type = 'Web Portal';

SELECT 
	CASE 
		WHEN dim_store_details.store_type = 'Web Portal' THEN 'Online'
		ELSE 'In-store'
	END AS sale_location,
	COUNT(*) AS sale_count,
	SUM(orders_table.product_quantity) AS total_quantity
FROM orders_table
JOIN dim_store_details ON orders_table.store_code = dim_store_details.store_code
GROUP BY sale_location
ORDER BY total_quantity DESC;

-- Calculating store type contribution to total sales

SELECT 
	store_type,
	SUM(dim_products.product_price * orders_table.product_quantity) AS revenue,
	SUM(dim_products.product_price * orders_table.product_quantity) / 
	(SELECT SUM(dim_products.product_price * orders_table.product_quantity) 
	 FROM orders_table 
	 JOIN dim_products ON orders_table.product_code = dim_products.product_code) * 100 AS revenue_percentage
FROM orders_table
JOIN dim_store_details ON orders_table.store_code = dim_store_details.store_code
JOIN dim_products ON orders_table.product_code = dim_products.product_code
GROUP BY store_type
ORDER BY revenue DESC;

-- Identifying peak sales months annually


SELECT year, month, SUM(dim_products.product_price * orders_table.product_quantity) AS annual_sales
FROM orders_table
JOIN dim_date_times ON orders_table.date_uuid = dim_date_times.date_uuid
JOIN dim_products ON orders_table.product_code = dim_products.product_code
GROUP BY year, month
ORDER BY annual_sales DESC;

-- Determining top-selling store types in Germany


SELECT 
	store_type,
	COUNT(*) AS sales_count
FROM orders_table
JOIN dim_store_details ON orders_table.store_code = dim_store_details.store_code
WHERE dim_store_details.country_code = 'DE'
GROUP BY store_type
ORDER BY sales_count DESC;

-- Calculating average sale completion time by year


WITH time_calc AS (
	SELECT 
		date_uuid,
		EXTRACT(YEAR FROM timestamp) AS year,
		LAG(timestamp) OVER (ORDER BY timestamp) AS prev_timestamp
	FROM dim_date_times
),
time_diffs AS (
	SELECT 
		year,
		AVG(timestamp - prev_timestamp) AS avg_time_diff
	FROM time_calc
	GROUP BY year
)
SELECT 
	year,
	avg_time_diff
FROM time_diffs
ORDER BY year;