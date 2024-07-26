CREATE DATABASE pizza_project;
USE pizza_project;

CREATE TABLE order_details (
order_details_id INT NOT NULL,
order_id INT NOT NULL,
pizza_id VARCHAR(50),
quantity INT NOT NULL
);

SELECT * 
FROM order_details;

CREATE TABLE orders (
order_id INT NOT NULL,
`date` date NOT NULL,
`time` VARCHAR(20)
);

SELECT * 
FROM orders;

CREATE TABLE pizzas (
pizza_id VARCHAR(50),
pizza_type_id VARCHAR(50),
size VARCHAR(20),
price FLOAT NOT NULL
);

SELECT * 
FROM pizzas;

CREATE TABLE pizza_types (
pizza_type_id VARCHAR(50) NOT NULL,
`name` VARCHAR(50),
category VARCHAR(20)
);

SELECT * 
FROM pizza_types;

LOAD DATA INFILE 'C:/order_details.csv'
INTO TABLE order_details
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

SHOW VARIABLES LIKE "secure_file_priv";

LOAD DATA INFILE 'C:/orders.csv'
INTO TABLE orders
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

LOAD DATA INFILE 'C:/pizza_types.csv'
INTO TABLE pizza_types
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

LOAD DATA INFILE 'C:/pizzas.csv'
INTO TABLE pizzas
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

# Exploratory Data Analysis

#Busiest months and days

SELECT monthname(`date`) AS peak_month, count(order_id) AS total_orders
FROM orders
GROUP BY monthname(`date`)
ORDER BY count(order_id) DESC;

SELECT dayname(`date`) AS day_of_week, count(order_id) AS total_orders
FROM orders
GROUP BY dayname(`date`)
ORDER BY count(order_id) DESC;

SELECT left(`time`, 2) AS time_of_order, count(order_id) AS total_orders
FROM orders
GROUP BY time_of_order
ORDER BY count(order_id) DESC;

#Best and worst selling pizzas

SELECT count(pizza_type_id)
FROM pizza_types;

SELECT count(pizza_id)
FROM pizzas;
#Best pizza by type and size
SELECT pizza_id, 
count(order_id) AS total_orders,
sum(quantity) AS total_pizzas_ordered
FROM order_details
GROUP BY pizza_id
ORDER BY count(order_id) DESC;

#Best pizza by type

SELECT pizza_type_id, 
count(order_id) AS total_orders,
sum(quantity) AS total_pizzas_ordered
FROM order_details AS od
JOIN pizzas AS pz
ON od.pizza_id =pz.pizza_id
GROUP BY pizza_type_id
ORDER BY total_orders DESC;

#Best pizza by category
SELECT category, 
count(order_id) AS total_orders,
sum(quantity) AS total_pizzas_ordered
FROM order_details AS od
JOIN pizzas AS pz
ON od.pizza_id =pz.pizza_id
JOIN pizza_types AS pt
ON pz.pizza_type_id = pt.pizza_type_id
GROUP BY category
ORDER BY total_orders DESC;

#most common size ordered

SELECT size, 
count(order_id) AS total_orders,
sum(quantity) AS total_pizzas_ordered
FROM order_details AS od
JOIN pizzas AS pz
ON od.pizza_id =pz.pizza_id
JOIN pizza_types AS pt
ON pz.pizza_type_id = pt.pizza_type_id
GROUP BY size
ORDER BY total_orders DESC;
#Average Order Value

WITH price_quantity AS 
(SELECT price, quantity, price*quantity AS order_value
FROM order_details AS od
JOIN pizzas AS pz
	ON od.pizza_id = pz.pizza_id)
SELECT AVG(order_value) 
FROM price_quantity
;

#pizzas per day served in restaurant

WITH pizzas_per_day AS
(SELECT distinct(`date`) AS only_date, count(od.order_id) AS total_orders,
sum(quantity) AS total_pizzas_ordered
FROM order_details AS od
JOIN orders AS ot
	ON od.order_id=ot.order_id
GROUP BY only_date
ORDER BY total_orders DESC)
SELECT max(total_pizzas_ordered), min(total_pizzas_ordered), avg(total_pizzas_ordered)
FROM pizzas_per_day;