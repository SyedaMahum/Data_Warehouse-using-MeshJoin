SELECT * FROM products_data.products_data;

SELECT COLUMN_NAME, DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'products_data'  
    AND TABLE_NAME = 'products_data';    

##UPDATE master_data.master_data SET productPrice = CAST(productPrice AS SIGNED) WHERE NOT productPrice REGEXP '^[0-9]+$';

UPDATE products_data.products_data
SET productPrice = REPLACE(productPrice, '$', '');

SELECT * FROM products_data.products_data LIMIT 10;
ALTER TABLE products_data.products_data
MODIFY productPrice DECIMAL(10, 2);

SELECT COLUMN_NAME, DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'products_data'
    AND TABLE_NAME = 'products_data';

select *
from products_data.products_data;

