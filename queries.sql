use METRO_DW;

-- Q1. Top Revenue-Generating Products on Weekdays and Weekends with Monthly Drill-Down
SELECT 
    EXTRACT(YEAR FROM f.FK_date) AS year,
    EXTRACT(MONTH FROM f.FK_date) AS month,
    CASE
        WHEN DAYOFWEEK(f.FK_date) BETWEEN 2 AND 6 THEN 'Weekday'  -- Monday (2) to Friday (6)
        ELSE 'Weekend'  -- Sunday (1) and Saturday (7)
    END AS day_type,
    f.FK_PRODUCT_ID,
    SUM(f.total_sale) AS total_revenue
FROM FACT f
GROUP BY year, month, day_type, f.FK_PRODUCT_ID
ORDER BY total_revenue DESC
LIMIT 5;


-- Q2. Trend Analysis of Store Revenue Growth Rate Quarterly for 2017
WITH quarterly_sales AS (
    SELECT f.FK_STORE_ID, EXTRACT(QUARTER FROM f.FK_date) AS quarter,SUM(f.total_sale) AS revenue
    FROM FACT f WHERE EXTRACT(YEAR FROM f.FK_date) = 2017 GROUP BY f.FK_STORE_ID, quarter )
SELECT qs.FK_STORE_ID, qs.quarter, qs.revenue,
LAG(qs.revenue) OVER (PARTITION BY qs.FK_STORE_ID ORDER BY qs.quarter) AS previous_quarter_revenue,
COALESCE(((qs.revenue - LAG(qs.revenue)
 OVER (PARTITION BY qs.FK_STORE_ID ORDER BY qs.quarter)) / LAG(qs.revenue) 
 OVER (PARTITION BY qs.FK_STORE_ID ORDER BY qs.quarter)) * 100, 0) AS growth_rate
FROM quarterly_sales qs
ORDER BY qs.FK_STORE_ID, qs.quarter;
-- checked no data of 2017 in csv hence empty --

-- Q3. Detailed Supplier Sales Contribution by Store and Product Name

SELECT 
    f.FK_STORE_ID,
    f.FK_SUPPLIER_ID,
    f.FK_PRODUCT_ID,
    SUM(f.total_sale) AS total_sales_contribution
FROM FACT f
GROUP BY f.FK_STORE_ID, f.FK_SUPPLIER_ID, f.FK_PRODUCT_ID
ORDER BY f.FK_STORE_ID, f.FK_SUPPLIER_ID, f.FK_PRODUCT_ID;



-- Q4. Seasonal Analysis of Product Sales Using Dynamic Drill-Down

SELECT f.FK_PRODUCT_ID,
CASE
        WHEN EXTRACT(MONTH FROM f.FK_date) IN (3, 4, 5) THEN 'Spring'
        WHEN EXTRACT(MONTH FROM f.FK_date) IN (6, 7, 8) THEN 'Summer'
        WHEN EXTRACT(MONTH FROM f.FK_date) IN (9, 10, 11) THEN 'Fall'
        ELSE 'Winter'  -- December, January, February
    END AS season,
    SUM(f.total_sale) AS total_sales
FROM FACT f
GROUP BY f.FK_PRODUCT_ID, season
ORDER BY f.FK_PRODUCT_ID, season;


-- Q5. Store-Wise and Supplier-Wise Monthly Revenue Volatility
WITH monthly_sales AS (
SELECT f.FK_STORE_ID, f.FK_SUPPLIER_ID,
EXTRACT(YEAR FROM f.FK_date) AS year, EXTRACT(MONTH FROM f.FK_date) AS month,
SUM(f.total_sale) AS total_revenue
FROM FACT f GROUP BY f.FK_STORE_ID, f.FK_SUPPLIER_ID, year, month)
SELECT ms.FK_STORE_ID, ms.FK_SUPPLIER_ID, ms.month, ms.total_revenue,
LAG(ms.total_revenue)
OVER (PARTITION BY ms.FK_STORE_ID, ms.FK_SUPPLIER_ID ORDER BY ms.month) AS previous_month_revenue,
    ((ms.total_revenue - LAG(ms.total_revenue) 
    OVER (PARTITION BY ms.FK_STORE_ID, ms.FK_SUPPLIER_ID ORDER BY ms.month)) / LAG(ms.total_revenue) 
    OVER (PARTITION BY ms.FK_STORE_ID, ms.FK_SUPPLIER_ID ORDER BY ms.month)) * 100 AS revenue_volatility
FROM monthly_sales ms
ORDER BY ms.FK_STORE_ID, ms.FK_SUPPLIER_ID, ms.month;


-- Q6. Top 5 Products Purchased Together Across Multiple Orders (Product Affinity Analysis)

SELECT 
    p.PRODUCT_ID, 
    p.PRODUCT_NAME, 
    SUM(f.Quantity) AS total_quantity_sold
FROM FACT f
JOIN PRODUCT p ON f.FK_PRODUCT_ID = p.PRODUCT_ID
GROUP BY p.PRODUCT_ID, p.PRODUCT_NAME
ORDER BY total_quantity_sold DESC
LIMIT 5;


-- Q7. Yearly Revenue Trends by Store, Supplier, and Product with ROLLUP

SELECT 
    f.FK_STORE_ID,
    f.FK_SUPPLIER_ID,
    f.FK_PRODUCT_ID,
    SUM(f.total_sale) AS yearly_revenue
FROM FACT f
GROUP BY f.FK_STORE_ID, f.FK_SUPPLIER_ID, f.FK_PRODUCT_ID WITH ROLLUP
ORDER BY f.FK_STORE_ID, f.FK_SUPPLIER_ID, f.FK_PRODUCT_ID;


 -- Q8. Revenue and Volume-Based Sales Analysis for Each Product for H1 and H2

SELECT 
    f.FK_PRODUCT_ID,
    SUM(CASE WHEN EXTRACT(MONTH FROM f.FK_date) BETWEEN 1 AND 6 THEN f.total_sale ELSE 0 END) AS revenue_h1,
    SUM(CASE WHEN EXTRACT(MONTH FROM f.FK_date) BETWEEN 1 AND 6 THEN f.Quantity ELSE 0 END) AS quantity_h1,
    SUM(CASE WHEN EXTRACT(MONTH FROM f.FK_date) BETWEEN 7 AND 12 THEN f.total_sale ELSE 0 END) AS revenue_h2,
    SUM(CASE WHEN EXTRACT(MONTH FROM f.FK_date) BETWEEN 7 AND 12 THEN f.Quantity ELSE 0 END) AS quantity_h2,
    SUM(f.total_sale) AS total_revenue,
    SUM(f.Quantity) AS total_quantity
FROM FACT f
GROUP BY f.FK_PRODUCT_ID
ORDER BY f.FK_PRODUCT_ID;


-- Q9. Identify High Revenue Spikes in Product Sales and Highlight Outliers

WITH daily_avg_sales AS ( SELECT f.FK_PRODUCT_ID, EXTRACT(DAY FROM f.FK_date) AS day,
	AVG(f.total_sale) AS avg_daily_sales
    FROM FACT f GROUP BY f.FK_PRODUCT_ID, day
)
SELECT f.FK_PRODUCT_ID, f.FK_date AS order_date, f.total_sale, das.avg_daily_sales,
CASE  WHEN f.total_sale > 2 * das.avg_daily_sales THEN 'Outlier' ELSE 'Normal' END AS sales_type
FROM FACT f JOIN daily_avg_sales das ON f.FK_PRODUCT_ID = das.FK_PRODUCT_ID AND 
EXTRACT(DAY FROM f.FK_date) = das.day
WHERE f.total_sale > 2 * das.avg_daily_sales;


-- Q10. Create a View STORE_QUARTERLY_SALES for Optimized Sales Analysis

CREATE VIEW STORE_QUARTERLY_SALES AS
SELECT 
    f.FK_STORE_ID,
    EXTRACT(QUARTER FROM f.FK_date) AS quarter,
    EXTRACT(YEAR FROM f.FK_date) AS year,
    SUM(f.total_sale) AS total_sales
FROM FACT f
GROUP BY f.FK_STORE_ID, quarter, year
ORDER BY f.FK_STORE_ID, year, quarter;

SELECT * FROM STORE_QUARTERLY_SALES;
