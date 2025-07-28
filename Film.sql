CREATE TABLE product_groups (
group_id serial PRIMARY KEY,
group_name VARCHAR (255) NOT NULL
);

CREATE TABLE products (
product_id serial PRIMARY KEY,
product_name VARCHAR (255) NOT NULL,
price DECIMAL (11, 2),
group_id INT NOT NULL,
FOREIGN KEY (group_id) REFERENCES product_groups (group_id)
);
ALTER TABLE product_groups
ADD PRIMARY KEY (group_id);

ALTER TABLE products
ADD FOREIGN KEY (group_id)
REFERENCES product_groups(group_id);

SELECT AVG(price) FROM products;

SELECT group_name, AVG(price) 
FROM products JOIN product_groups USING (group_id)
GROUP BY group_name;

SELECT product_name, price, group_name, 
 AVG(price) OVER (PARTITION BY group_name)
FROM products JOIN product_groups USING (group_id);

Function_name() OVER (
 [PARTITION BY column_to_group_by]
 [ORDER BY column_to_order_by]
);

SELECT 
 product_name, 
 price, 
 group_id,
 RANK() OVER (
 PARTITION BY group_id
 ORDER BY price DESC
 ) AS price_rank
FROM products; 

SELECT
 p.product_id,
 p.product_name,
 pg.group_name,
 p.price,
 RANK() OVER (
 PARTITION BY p.group_id
 ORDER BY p.price DESC
 ) AS rank_in_group
FROM
 products p
JOIN
 product_groups pg ON p.group_id = pg.group_id;

SELECT
 product_id,
 product_name,
 group_name,
 price,
 RANK() OVER (
 PARTITION BY group_name
 ORDER BY price
 ) AS rank_in_group_by_price
FROM
 products
INNER JOIN
 product_groups
USING (group_id);

SELECT
 product_id,
 product_name,
 group_name,
 price,
 DENSE_RANK() OVER (
 PARTITION BY group_name
 ORDER BY price
 ) AS dense_rank_in_group
FROM
 products
INNER JOIN
 product_groups
USING (group_id);

SELECT
 product_id,
 product_name,
 group_name,
 price,
 FIRST_VALUE(price) OVER (
 PARTITION BY group_name
 ORDER BY price
 ) AS first_price_in_group
FROM
 products
INNER JOIN
 product_groups
USING (group_id);

SELECT
 product_name,
 group_name,
 price,
 RANK() OVER (PARTITION BY group_name ORDER BY price DESC) AS 
price_rank,
 ROW_NUMBER() OVER (PARTITION BY group_name ORDER BY price
DESC) AS row_num
FROM
 products
JOIN
 product_groups USING (group_id);

SELECT
 product_name,
 RANK() OVER (PARTITION BY group_name ORDER BY price DESC) AS 
rank1,
 DENSE_RANK() OVER (PARTITION BY group_name ORDER BY price
DESC) AS rank2
FROM products;

SELECT
 product_name,
 RANK() OVER w AS rank1,
 DENSE_RANK() OVER w AS rank2
FROM products
WINDOW w AS (PARTITION BY group_name ORDER BY price
DESC);


SELECT * FROM film;