
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
