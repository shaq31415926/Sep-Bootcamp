---Selects all columns and all rows from the table online transactions

select ot.*
from bootcamp.online_transactions ot;

---Selects all columns and first ten rows from the table online transactions

select ot.*
from bootcamp.online_transactions ot
limit 10;


---Selects all columns from the table stock desc

select *
from bootcamp.stock_description sd;


-- select specific columns from the online transactions table
select customer_id,
	   price,
	   quantity
from bootcamp.online_transactions ot;

-- number of rows the stock desc table contains
select count(*) as number_rows
from bootcamp.stock_description sd;
-- there are 3,952 rows of data


-- how many rows does the online transactions table contain?
select count(*) as number_rows
from bootcamp.online_transactions ot;
-- there are 541k rows of data

-- total items sold by a company
-- if you want to sum only the positive quantities add a where condition 
select sum(quantity) as total_items_sold
from bootcamp.online_transactions ot
where quantity > 0
;

-- average price of products, rounded to 2 d.p
select round(avg(price), 2) as average_price
from bootcamp.online_transactions ot 
;


-- average price of products for France
select round(avg(price), 2) as average_price
from bootcamp.online_transactions ot 
where country = 'Denmark'
;

-- distinct values of a column of our choice
select distinct Country
from bootcamp.online_transactions ot;

-- distinct number of countries
select count(distinct Country)
from bootcamp.online_transactions ot;

-- how many rows of data do we have for Hong Kong
select count(*)
from bootcamp.online_transactions ot 
where country = 'Hong Kong';

-- how many rows of data do we have for United Kingdom
select count(*)
from bootcamp.online_transactions ot 
where country = 'United Kingdom';

-- select all columns where county is United Kingom
select *
from bootcamp.online_transactions ot 
where country = 'United Kingdom';


-- the minimum price
select min(price) as min_price
from bootcamp.online_transactions ot;

-- the maximum price
select max(price) as max_price
from bootcamp.online_transactions ot;

-- select the min, max and average price
select  min(price) as min_price,
		max(price) as max_price,
		round(avg(price), 2) as average_price
from bootcamp.online_transactions ot;

-- select the min, max and average price for Germany
select  min(price) as min_price,
		max(price) as max_price,
		round(avg(price), 2) as average_price
from bootcamp.online_transactions ot
where country = 'Germany';


























