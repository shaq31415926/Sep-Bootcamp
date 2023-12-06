-- preview the tables we are working with
select *
from bootcamp.online_transactions ot 
limit 10;

select *
from bootcamp.stock_description sd 
limit 10;

-- number of rows of each table
select count(*)
from bootcamp.online_transactions ot 
;

select count(*)
from bootcamp.stock_description sd 
;

-- number of stock codes in online trans table
select count(distinct stock_code)
from bootcamp.online_transactions ot 
;

-- in an ideal world we would have one stock code and one description in the stock desc table
select count(distinct stock_code)
from bootcamp.stock_description sd 
;

select count(stock_code)
from bootcamp.stock_description sd 
;

-- question: how many stock codes have the description ?
select count(*)
from bootcamp.stock_description sd 
where description = '?';

select *
from bootcamp.stock_description sd 
where description = '?';

-- let's look at an example
select *
from bootcamp.stock_description sd 
where stock_code = '16020C';

-- for reference: joining the table with and without ?
select *
from bootcamp.online_transactions ot 
left join (select *
		   from bootcamp.stock_description
		   where description <> '?') sd on ot.stock_code = sd.stock_code
where ot.stock_code = '16020C';

select *
from bootcamp.online_transactions ot 
left join bootcamp.stock_description sd on ot.stock_code = sd.stock_code
where ot.stock_code = '16020C';


-- identify stock codes that have multiple descriptions
-- using a left join we can see there is a null value for description
select *
from bootcamp.online_transactions ot 
left join bootcamp.stock_description sd on ot.stock_code = sd.stock_code
where ot.stock_code = '22686';

-- doing an inner join will drop any transactions with a stock code that is not in the stock desc table

select *
from bootcamp.online_transactions ot 
join bootcamp.stock_description sd on ot.stock_code = sd.stock_code
where ot.stock_code = '22686';

--identify the stock codes which do not exist in the stock description table
select ot.*,
	   sd.description 
from bootcamp.online_transactions ot 
left join bootcamp.stock_description sd on ot.stock_code = sd.stock_code
where description is null;

-- 165 stock codes do not exist in the stock code table
select count(distinct ot.stock_code)
from bootcamp.online_transactions ot 
left join bootcamp.stock_description sd on ot.stock_code = sd.stock_code
where description is null;

-- list of stock codes that do not exist in the stock description table
select distinct ot.stock_code
from bootcamp.online_transactions ot 
left join bootcamp.stock_description sd on ot.stock_code = sd.stock_code
where description is null;

-- double check if that is correct
select *
from bootcamp.stock_description sd 
where stock_code = 'gift_0001_40';

select *
from bootcamp.stock_description sd 
where stock_code = '84029E';

select *
from bootcamp.stock_description sd 
where stock_code = '22900';

select *
from bootcamp.stock_description sd 
where stock_code = '21730';


-- example of a missing stock code, that exists in the left table but not stock desc table
select *
from bootcamp.stock_description sd
where stock_code = '22686';

select *
from bootcamp.stock_description sd
where stock_code = '22423';


-- recap: identifying stocks with description
select count(distinct ot.stock_code)
from bootcamp.online_transactions ot 
left join bootcamp.stock_description sd on ot.stock_code = sd.stock_code
where description is null;

--165 stock codes that dont exist in our stock description table

select distinct ot.stock_code
from bootcamp.online_transactions ot 
left join bootcamp.stock_description sd on ot.stock_code = sd.stock_code
where description is null;

-- this would give you the # of transactions with stock codes with missing description
select count(*)
from bootcamp.online_transactions ot 
left join bootcamp.stock_description sd on ot.stock_code = sd.stock_code
where description is null;

select *
from bootcamp.online_transactions ot 
left join bootcamp.stock_description sd on ot.stock_code = sd.stock_code
where description is null
order by ot.stock_code;


-- top 10 best selling products
select sum(quantity)
from bootcamp.online_transactions ot 
where stock_code = '21705';
-- this was sold 1,439 times 

-- step 1, aggergate the online trans table to identify the amount sold of every stop code, and get the top 10
select stock_code,
	   sum(quantity) as total_sold
from bootcamp.online_transactions ot 
group by stock_code
order by total_sold desc
limit 10;

-- two ways to understand the items sold with their description
select ot.stock_code,
	   sd.description,
	   sum(quantity) as total_sold
from bootcamp.online_transactions ot 
left join bootcamp.stock_description sd on ot.stock_code = sd.stock_code
group by ot.stock_code, sd.description 
order by total_sold desc
limit 10;


select sd.description,
	   sum(quantity) as total_sold
from bootcamp.online_transactions ot 
left join bootcamp.stock_description sd on ot.stock_code = sd.stock_code
where description <> '?'
group by sd.description 
order by total_sold desc
limit 10;

-- if you group by description, this will sum all transactions with desc ? and will ignore the stock code
select sum(quantity)
from bootcamp.online_transactions ot 
left join bootcamp.stock_description sd on ot.stock_code = sd.stock_code
where description = '?'
;

-- for a country of choice, what are the top 10 best selling
select sd.description,
	   sum(quantity) as total_sold
from bootcamp.online_transactions ot 
left join bootcamp.stock_description sd on ot.stock_code = sd.stock_code
where description <> '?' and country = 'Germany'
group by sd.description 
order by total_sold desc
limit 10;

-- best selling products for every country
select *
from
(select country,
	   ot.stock_code,
	   description,
	   sum(quantity) as total_quantity,
	   rank() over (partition by country order by total_quantity desc) as rank_number
from bootcamp.online_transactions ot 
left join bootcamp.stock_description sd on ot.stock_code = sd.stock_code
--where country in ('Germany', 'United Kingdom')
where description not in ('POSTAGE', '?')
group by country, ot.stock_code, description
order by country, total_quantity desc)
where rank_number < 6;

-- how do you identify stock codes that contain letter?
-- my lame version
select count(distinct stock_code)
from bootcamp.online_transactions ot
where (
UPPER(stock_code) like '%A%' OR
UPPER(stock_code) like '%B%' or
UPPER(stock_code) like '%C%' or
UPPER(stock_code) like '%D%' or
UPPER(stock_code) like '%E%' or
UPPER(stock_code) like '%F%' or
UPPER(stock_code) like '%G%' or
UPPER(stock_code) like '%H%' or
UPPER(stock_code) like '%I%' or
UPPER(stock_code) like '%J%' or
UPPER(stock_code) like '%K%' or
UPPER(stock_code) like '%L%' or
UPPER(stock_code) like '%M%' or
UPPER(stock_code) like '%N%' or
UPPER(stock_code) like '%O%' or
UPPER(stock_code) like '%P%' or
UPPER(stock_code) like '%Q%' or
UPPER(stock_code) like '%R%' or
UPPER(stock_code) like '%S%' or
UPPER(stock_code) like '%T%' or
UPPER(stock_code) like '%U%' or
UPPER(stock_code) like '%V%' or
UPPER(stock_code) like '%W%' or
UPPER(stock_code) like '%X%' or
UPPER(stock_code) like '%Y%' or
UPPER(stock_code) like '%Z%')
;

-- Neringa's awesome code to identify stock codes that contain a letter, 1124 stocks
select count(distinct stock_code)
from bootcamp.online_transactions ot
where stock_code similar to '%[A-Za-z]%';

select distinct stock_code
from bootcamp.online_transactions ot
where stock_code similar to '%[A-Za-z]%';


-- Code to identify stock codes that start with a letter, 33 stocks
select count(distinct stock_code)
from bootcamp.online_transactions ot
where stock_code similar to '[A-Za-z]%';

select distinct stock_code
from bootcamp.online_transactions ot
where stock_code similar to '[A-Za-z]%';


