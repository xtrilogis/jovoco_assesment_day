
-- if dates were current
-- SELECT year_quarter FROM gold_dim_date where DATE(date) = DATE('now','-3 months')
-- 25Q1 hat nur 4 Bestellungen
WITH full_table AS (
    SELECT 
		s.region,
		p.title,
		d.year_quarter,
		d.date, 
		f.revenue 
    FROM gold_fact_sales f
    JOIN gold_dim_stores s   ON f.store_id = s.store_id
    JOIN gold_dim_products p ON f.product_id = p.product_id
    JOIN gold_dim_date d     ON f.date_id = d.date_id
), ranked_table as (
SELECT
        region,
        title AS product,
        SUM(revenue) AS total_revenue,
        RANK() OVER (PARTITION BY region ORDER BY SUM(revenue) DESC) AS rnk
    FROM full_table 
    WHERE year_quarter = (
	    SELECT year_quarter
	    FROM full_table
	    ORDER BY date DESC
	    LIMIT 1)
    GROUP BY region, title
)
SELECT *
FROM ranked_table
WHERE rnk <= 5;

-- High Mid Low Value Kunden
WITH groups as (
SELECT
	c.name as customer,
    SUM(revenue) AS total_revenue,
    NTILE(3) OVER (ORDER BY SUM(revenue) DESC) AS value_segment
FROM gold_fact_sales f
JOIN gold_dim_date d ON f.date_id = d.date_id
Join gold_dim_customers c on c.customer_id  = f.customer_id  
WHERE d.date >= DATE('2025-01-08','-12 months') -- TODO: dynamisch
GROUP BY f.customer_id
)
Select customer, total_revenue,
CASE
    WHEN value_segment = 1 THEN "High"
    WHEN value_segment = 2 THEN "Mid"
    WHEN value_segment = 3 THEN "Low Value"
    ELSE "No Group"
END as "Group"
from groups;


-- Produkt Relationen
With correlation AS (
SELECT
    CASE 
        WHEN i1.product_id < i2.product_id 
        THEN i1.product_id 
        ELSE i2.product_id 
    END AS product_a,
    CASE 
        WHEN i1.product_id < i2.product_id 
        THEN i2.product_id 
        ELSE i1.product_id 
    END AS product_b,
    COUNT(*) AS freq
FROM gold_fact_sales i1
JOIN gold_fact_sales i2 
    ON i1.order_id = i2.order_id 
   AND i1.product_id < i2.product_id
GROUP BY product_a, product_b
ORDER BY freq DESC
LIMIT 10
)
Select 
 gdp.title as product_a,
 gdp1.title as product_b,
 c.freq 
from correlation c
join gold_dim_products gdp on gdp.product_id = c.product_a
join gold_dim_products gdp1 on gdp1.product_id = c.product_b;

-- More than one quarter
WITH customer_quarters AS (
    SELECT customer_id, COUNT(DISTINCT quarter) AS quarters_ordered
    FROM gold_fact_sales f
    JOIN gold_dim_date d ON f.date_id = d.date_id
    GROUP BY customer_id
)
SELECT
    100.0 * SUM(CASE WHEN quarters_ordered > 1 THEN 1 ELSE 0 END) / COUNT(*)
    AS pct_multi_quarter_customers
FROM customer_quarters;

-- Verdrängung
-- siehe Streamlit
