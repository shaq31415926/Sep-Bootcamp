-- select the first ten rows of the online transactions table

select *
from bootcamp.online_transactions ot 
order by customer_id desc, invoice_date 
limit 100;



-- select the first ten rows of the stock description table
select *
from bootcamp.stock_description sd 
limit 10;

-- how many rows does the online transactions table contain?
select count(*)
from bootcamp.online_transactions;
-- 541k rows in the online transactions table

-- select the distinct customer_id and country
select distinct customer_id,
				country
from bootcamp.online_transactions ot;


-- how many rows does the stock description table contain?
select count(*) as number_rows
from bootcamp.stock_description sd;
-- 4k rows

-- how many customers does the online transaction contain
select count(distinct customer_id) as number_customers
from bootcamp.online_transactions ot;
-- we have 4,373 customers

-- how many invoices does the online transaction table contain??
select count(distinct invoice) as number_invoices
from bootcamp.online_transactions ot;
-- 25,900 invoices

-- how many items were sold?
select count(distinct stock_code)
from bootcamp.online_transactions ot;
-- 4070 items were sold

-- how many stock codes do we have in the stock desc table?
select count(distinct stock_code)
from bootcamp.stock_description sd;
-- but we only have 3,905 stock codes in this table

select count(stock_code)
from bootcamp.stock_description sd;
-- but we have 3,952 rows of data - WHY?!

-- using the group by function to identify which stock codes appear multiple times
select stock_code
from bootcamp.stock_description
group by stock_code
-- this having function is used with groups by
having count(*) > 1

--pick an example of where stock_code is greater than 1
select *
from bootcamp.stock_description sd 
where stock_code = '16020C';
-- aah, those pesky question marks.


-- number of rows where desc is ?
select count(*) as number_desc
from bootcamp.stock_description sd 
where description = '?';

-- how many stock descriptions contain ? in then
select count(*) as number_desc
from bootcamp.stock_description sd 
-- the like function looks for if ? appears in description
where description like '%?%';


-- when was the first and last invoice?
select min(invoice_date) as min_invoice_date,
       max(invoice_date) as max_invoice_date
from bootcamp.online_transactions ot;


--- identifying the description of the most expensive products
select max(price)
from bootcamp.online_transactions ot 
where country = 'Spain';


select stock_code
from bootcamp.online_transactions ot 
where country = 'EIRE' and price=(select max(price)
								   from bootcamp.online_transactions ot 
								   where country = 'EIRE');
								  
select description
from bootcamp.stock_description 
where stock_code = 'M';

-- average price of items sold in Italy, rounded to 2 d.p

select round(avg(Price), 2)
from bootcamp.online_transactions ot
where country = 'Italy';

-- number of unique customers in Italy
select count(distinct customer_id)
from bootcamp.online_transactions ot
where country = 'Italy';


-- average price of every country using the group by
select Country,
	   count(distinct customer_id) as total_customers,
	   round(avg(Price), 2) as average_price,
	   count(distinct invoice) as number_invoices,
	   sum(quantity) as total_sales
from bootcamp.online_transactions ot
--where country = 'Italy'
group by Country
order by total_sales desc;

-- the number of rows where country is in Italy and United Kingdom
select count(*)
from bootcamp.online_transactions ot
where country in ('Italy', 'United Kingdom')
;


-- write a group by statement that gives the total number of rows per country
select country,
	   count(*) as number_rows
from bootcamp.online_transactions ot 
--where country in ('Italy', 'United Kingdom')
group by country
order by number_rows;


-- write a group by statement that gives the total number of customers per country
select country,
	   count(distinct customer_id) as number_customers
from bootcamp.online_transactions ot 
--where country in ('Germany', 'Italy')
group by country
order by number_customers desc;









