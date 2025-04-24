
-- What are the sales patterns over the years?

SELECT
YEAR(order_date) AS order_year,
SUM(sales_amount) AS total_sales,
COUNT(DISTINCT customer_key) AS total_customers,
SUM(quantity) AS total_quantity
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY YEAR(order_date)
ORDER BY YEAR(order_date)
GO



-- Which months had the highest sales?

SELECT
MONTH(order_date) AS order_month,
SUM(sales_amount) AS total_sales,
COUNT(DISTINCT customer_key) AS total_customers,
SUM(quantity) AS total_quantity
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY MONTH(order_date)
ORDER BY total_sales DESC
GO


SELECT
DATETRUNC(MONTH, order_date) AS order_year_month,
SUM(sales_amount) AS total_sales,
COUNT(DISTINCT customer_key) AS total_customers,
SUM(quantity) AS total_quantity
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY DATETRUNC(MONTH, order_date)
ORDER BY DATETRUNC(MONTH, order_date)
GO


SELECT
FORMAT(order_date, 'yyyy-MMM') AS order_year_month,
SUM(sales_amount) AS total_sales,
COUNT(DISTINCT customer_key) AS total_customers,
SUM(quantity) AS total_quantity
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY FORMAT(order_date, 'yyyy-MMM')
ORDER BY FORMAT(order_date, 'yyyy-MMM')
GO



--- Calculate the total sales per month
--- and the running total sales over time

SELECT
DATETRUNC(MONTH, order_date) AS order_month,
SUM(sales_amount) AS total_sales
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY DATETRUNC(MONTH, order_date)
ORDER BY DATETRUNC(MONTH, order_date)
GO

-- running total sales

SELECT
order_month,
total_sales,
-- Window Function: Defalut Window Frame - Between unbounded preceding and current row
SUM(total_sales) OVER (ORDER BY order_month) as running_total_sales
FROM
(
SELECT
DATETRUNC(MONTH, order_date) AS order_month,
SUM(sales_amount) AS total_sales
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY DATETRUNC(MONTH, order_date)
)t
GO


-- To partition the data for each year so that the sales reset at the start of each year

SELECT
order_month,
total_sales,
-- Window Function: Defalut Window Frame - Between unbounded preceding and current row
SUM(total_sales) OVER (PARTITION BY YEAR(order_month) ORDER BY order_month) as running_total_sales
FROM
(
SELECT
DATETRUNC(MONTH, order_date) AS order_month,
SUM(sales_amount) AS total_sales
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY DATETRUNC(MONTH, order_date)
)t
GO


-- To find the moving average by year

SELECT
order_year,
total_sales,
avg_price,
-- Window Function: Defalut Window Frame - Between unbounded preceding and current row
SUM(total_sales) OVER (ORDER BY order_year) as running_total_sales,
AVG(avg_price) OVER (ORDER BY order_year) as moving_average
FROM
(
SELECT
DATETRUNC(year, order_date) AS order_year,
SUM(sales_amount) AS total_sales,
AVG(price) AS avg_price
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY DATETRUNC(year, order_date)
)t
GO



/* Analyze the yearly performance of products by comparing their sales to both 
the average sales performance of the product and the previous year's sales*/



WITH yearly_product_sales AS(
SELECT
YEAR(f.order_date) AS order_year,
p.product_name,
SUM(f.sales_amount) AS current_sales
FROM gold.fact_sales f
LEFT JOIN
gold.dim_products p
ON p.product_key = f.product_key
WHERE f.order_date IS NOT NULL
GROUP BY YEAR(f.order_date),
p.product_name
)

SELECT 
order_year,
product_name,
current_sales,
AVG(current_sales) OVER (PARTITION BY product_name) avg_sales,
CASE
WHEN current_sales - AVG(current_sales) OVER (PARTITION BY product_name) > 0 THEN 'Above Avg'
WHEN current_sales - AVG(current_sales) OVER (PARTITION BY product_name) < 0 THEN 'Below Avg'
ELSE 'Avg'
END avg_change,
LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) py_sales,
current_sales - LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) AS diff_py,
CASE 
WHEN current_sales - LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) > 0 THEN 'Increase'
WHEN current_sales - LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) < 0 THEN 'Decrease'
ELSE 'No Change'
END py_change
FROM yearly_product_sales
ORDER BY product_name, order_year
GO





-- Which categories contribute the most to the overall sales?

WITH category_sales AS (

SELECT 
category,
SUM(sales_amount) total_sales
FROM gold.fact_sales f
LEFT JOIN
gold.dim_products p 
ON f.product_key = p.product_key
GROUP BY category
)

SELECT
category,
total_sales,
SUM(total_sales) OVER () overall_sales,
CONCAT(ROUND((CAST (total_sales AS FLOAT) / SUM(total_sales) OVER ()) * 100 , 2),'%') AS percentage_of_total
FROM category_sales
ORDER BY total_sales DESC
GO



/* Segment products intocost ranges and
count how many products fall into each segment */


WITH product_segments AS (

SELECT 
product_key,
product_name,
cost,
CASE 
WHEN cost < 100 THEN 'Below 100'
WHEN cost BETWEEN 100 AND 500 THEN '100 - 500'
WHEN cost BETWEEN 500 AND 1000 THEN '500 - 1000'
ELSE 'Above 1000'
END cost_range
FROM gold.dim_products
)

SELECT
cost_range,
COUNT(product_key) AS total_products
FROM product_segments
GROUP BY cost_range
ORDER BY total_products DESC
GO



/* Group customers into three segments based on their spending behavior:
 - VIP : Customers with at least 12 months of history and spending more than 5000.
 - Regular : Customers with at least 12 months of history but spending less than 5000.
 - New : Customers with a lifespan less than 12 months
And find the total number of customers by each group.
*/

WITH customer_spending AS(

SELECT 
c.customer_key,
SUM(f.sales_amount) AS total_spending,
MIN(f.order_date) AS first_order,
MAX(f.order_date) AS last_order,
DATEDIFF(MONTH, MIN(f.order_date), MAX(f.order_date)) AS lifespan
FROM gold.fact_sales f
LEFT JOIN
gold.dim_customers c
ON c.customer_key = f.customer_key
GROUP BY c.customer_key
)

SELECT
customer_segment,
Count(customer_key) AS total_customers
FROM (
SELECT
customer_key,
CASE
WHEN lifespan >= 12 AND total_spending > 5000 THEN 'VIP'
WHEN lifespan >= 12 AND total_spending <= 5000 THEN 'Regular'
ELSE 'New'
END customer_segment
FROM customer_spending ) t
GROUP BY customer_segment
ORDER BY total_customers DESC
GO




