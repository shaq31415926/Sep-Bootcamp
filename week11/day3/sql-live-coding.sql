-- preview the first ten rows of data

select *
from bootcamp.online_transactions ot 
limit 10;

select *
from bootcamp.stock_description sd 
limit 10;

-- identify the top 10 countries that have the highest average price
select country,
	   round(avg(price), 2) as avg_price,
	   -- for reference
	   count(distinct customer_id) as number_customers
from bootcamp.online_transactions ot 
group by country
order by avg_price desc
limit 10;

-- identify the country with the most customers
select country,
	   count(distinct customer_id) as number_customers
from bootcamp.online_transactions ot 
group by country
order by number_customers desc
limit 1;

-- identify the country with the SECOND most customers
-- option 1, remove the top country
select country,
	   count(distinct customer_id) as number_customers
from bootcamp.online_transactions ot 
where country not in (select country
						from bootcamp.online_transactions ot 
						group by country
						order by number_customers desc
						limit 1)
group by country
order by number_customers desc
limit 1;


-- option 2, use row number
select country,
	   number_customers
from (
	select country,
		   count(distinct customer_id) as number_customers,
		   row_number() over (order by number_customers desc) as row_number
	from bootcamp.online_transactions ot 
	group by country
	order by number_customers desc)
where row_number = 2
;

-- option 3, use rank
select country,
	   number_customers
from (
	select country,
		   count(distinct customer_id) as number_customers,
		   rank() over (order by number_customers desc) as rank
	from bootcamp.online_transactions ot 
	group by country
	order by number_customers desc)
where rank = 2;

-- option 4, use dense rank - this specifies the unique rank number
select country,
	   number_customers
from (
	select country,
		   count(distinct customer_id) as number_customers,
		   dense_rank() over (order by number_customers desc) as rank
	from bootcamp.online_transactions ot 
	group by country
	order by number_customers desc)
where rank = 2;


-- customers that spent the most
select customer_id,
	   round(sum(price*quantity), 2) as total_order_value
from bootcamp.online_transactions ot 
where customer_id <> ''
group by customer_id
order by total_order_value desc
limit 1;

--customers that spend the most per country
select country,
	   customer_id,
	   	total_order_value
from (
		select country,
			   customer_id,
			   round(sum(price*quantity), 2) as total_order_value,
			   rank() over (partition by country order by total_order_value desc)
		from bootcamp.online_transactions ot 
		where customer_id <> ''
		group by country, customer_id
		order by country)
where rank = 1
order by total_order_value desc
;

