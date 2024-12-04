select * from transactions.transactions;

select * from customer_data.customer_data;

SELECT t.`ORDER ID`, t.`ORDER DATE`, t.ProductID, t.`Quantity Ordered`, t.customer_id,
       c.customer_name, c.gender, t.time_id
FROM transactions.transactions t
JOIN customer_data.customer_data c ON t.customer_id = c.customer_id;


CREATE TABLE transactions.enriched_transactions AS
SELECT 
    t.`ORDER ID`,
    t.`ORDER DATE`,
    t.ProductID,
    t.`Quantity Ordered`,
    t.customer_id,
    c.customer_name,
    c.gender,
    t.time_id
FROM 
    transactions.transactions t
JOIN 
    customer_data.customer_data c
ON 
    t.customer_id = c.customer_id;


select *
from transactions.enriched_transactions;

DESCRIBE products_data.products_data;  -- This will list all columns in the products_data table
DESCRIBE transactions.enriched_transactions;  -- This will list all columns in the enriched_transactions table
use transactions;
UPDATE enriched_transactions
SET `Order Date` = STR_TO_DATE(`Order Date`, '%m/%d/%Y %H:%i')
WHERE `Order Date` LIKE '%/%/% %:%';



SELECT COLUMN_NAME, DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'customer_data'
    AND TABLE_NAME = 'customer_data';