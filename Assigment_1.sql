drop database if exists assigmet_1;
create database assigmet_1;
use assigmet_1;
set global local_infile = 1;

create table products(
product_id int unique,
product_name varchar(15) ,
category varchar(15),
price float,
supplier_id int 
);

create table suppliers(
supplier_id int unique,
supplier_name varchar(30),
contact_name varchar(30),
country varchar(40)
);

create table orders(
order_id int unique,
customer_id int,
order_date int,
status varchar(10)
);

create table order_items(
order_item_id int unique,
order_id int,
product_id int,
quantity int,
total_price float
);

create table customers(
customer_id int unique,
first_name varchar(15),
last_name varchar(15),
email varchar(30),
city varchar(30),
country varchar(50)
);

LOAD DATA LOCAL INFILE '/Users/sasha0910/Programming/MySQL/Assigment_1/mysql_join_data/customers.csv' INTO TABLE customers
fields terminated by ","
ignore 1 rows;

LOAD DATA LOCAL INFILE '/Users/sasha0910/Programming/MySQL/Assigment_1/mysql_join_data/order_items.csv' INTO TABLE order_items
fields terminated by ","
ignore 1 rows;

LOAD DATA LOCAL INFILE '/Users/sasha0910/Programming/MySQL/Assigment_1/mysql_join_data/orders.csv' INTO TABLE orders
fields terminated by ","
ignore 1 rows;

LOAD DATA LOCAL INFILE '/Users/sasha0910/Programming/MySQL/Assigment_1/mysql_join_data/products.csv' INTO TABLE products 
fields terminated by ","	
ignore 1 rows;

LOAD DATA LOCAL INFILE '/Users/sasha0910/Programming/MySQL/Assigment_1/mysql_join_data/suppliers.csv' INTO TABLE suppliers
fields terminated by ","
ignore 1 rows;

with cte as(
select product_name, category, price, supplier_id, order_item_id, quantity, total_price,order_id
from products p
join order_items oi
on oi.product_id = p.product_id
)
select avg(price) as avg_price, avg(total_price) as avg_total_price, category
from cte cte
join suppliers s
on cte.supplier_id = s.supplier_id
join orders o
on o.order_id = cte.order_id
join customers c
on c.customer_id = o.customer_id
where 200 < price and quantity > (
	select avg(quantity)
    from order_items
    )
group by category
having min(total_price) < 2000
union
select avg(price) as avg_price, avg(total_price) as avg_total_price, category
from products p 
join order_items io
on io.product_id = p.product_id
group by category
order by avg_total_price
limit 3;
    


