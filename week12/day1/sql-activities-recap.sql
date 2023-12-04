-- preview the first 20 rows of the online transactions table
select *
from bootcamp.online_transactions ot 
limit 20;

-- preview the first 20 rows of the stock description table
select *
from bootcamp.stock_description ot 
order by stock_code
limit 20;

-- The online transactions table has 542k rows of data
select count(*)
from bootcamp.online_transactions ot;

-- The online transactions table has 4k rows of data
select count(*)
from bootcamp.stock_description sd;

-- The online transactions table has 4,070 stocks
select count(distinct stock_code)
from bootcamp.online_transactions ot;

-- The online transactions table has 3,905 unique stock codes

select count(stock_code)
from bootcamp.stock_description sd;

select count(distinct stock_code)
from bootcamp.stock_description sd;


--- Investigating stocks with multiple descriptions
select stock_code,
	   count(*) as number_desc
from bootcamp.stock_description sd 
group by stock_code
-- having is used with group by to filter based on an aggregate function
having count(*) > 1
;

-- number of stock codes with multiple descriptions

select count(*)
from (
	select stock_code,
		   count(*) as number_desc
	from bootcamp.stock_description sd 
	group by stock_code
	having count(*) > 1)
;
-- 47 stock codes that have multiple desc

-- why do they have multiple description?
select *
from bootcamp.stock_description sd 
where stock_code = '16020C';


select *
from bootcamp.stock_description sd 
where stock_code in (select stock_code
					from bootcamp.stock_description sd 
					group by stock_code
					having count(*) > 1)
order by stock_code;
				

-- 1,257 transactions with the stock code POST
-- this selects all transactions where transaction is POST
select *
from bootcamp.online_transactions ot
where stock_code='POST';

-- this counts how many transactions have the stock code POST
select count(*)
from bootcamp.online_transactions sd 
where stock_code='POST';


-----left join description to our online description table
select ot.*,
	   sd.description 
from bootcamp.online_transactions ot
left join bootcamp.stock_description sd on ot.stock_code = sd.stock_code;

-- check the # of rows for our new table with stock description joined
select count(*) 
from bootcamp.online_transactions ot
left join bootcamp.stock_description sd on ot.stock_code = sd.stock_code;
-- we now have 552k rows of data, but our online trans table has 542k 

-- for reference only: you would join stock description table without the ?
select ot.*,
	   sd.description
from bootcamp.online_transactions ot 
left join (select *
		   from bootcamp.stock_description
		   where description <> '?') sd on ot.stock_code = sd.stock_code 
where ot.stock_code = '16207B';

select count(*)
from bootcamp.online_transactions ot 
left join (select *
		   from bootcamp.stock_description
		   where description <> '?') sd on ot.stock_code = sd.stock_code ;


-----join description to our online description table
select ot.*,
	   sd.description 
from bootcamp.online_transactions ot
join bootcamp.stock_description sd on ot.stock_code = sd.stock_code;

-- check the # of rows for our new table with stock description joined
select count(*) 
from bootcamp.online_transactions ot
join bootcamp.stock_description sd on ot.stock_code = sd.stock_code;
-- when we do an inner join, we have less rows than we do a left join



