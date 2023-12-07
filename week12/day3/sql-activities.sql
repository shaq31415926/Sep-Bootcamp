-- how to get month and year, using date part and cast
select invoice_date,
	   date_part(year, cast(invoice_date as Datetime)) as invoice_year,
	   date_part(month, cast(invoice_date as Datetime)) as invoice_month
from bootcamp.online_transactions ot 
limit 10;


-- reminder, of the range of invoice dates
select min(invoice_date),
	   max(invoice_date)
from bootcamp.online_transactions ot;


--number of invoices per month
select date_part(year, cast(invoice_date as Datetime)) as invoice_year,
	   date_part(month, cast(invoice_date as Datetime)) as invoice_month,
	   count(distinct invoice) as number_invoices
from bootcamp.online_transactions ot 
group by invoice_year, invoice_month
order by invoice_year, invoice_month
;

-- number of invoices per month in year 2011
select date_part(month, cast(invoice_date as Datetime)) as invoice_month,
	   count(distinct invoice) as number_invoices
from bootcamp.online_transactions ot 
where date_part(year, cast(invoice_date as Datetime)) = 2011
group by invoice_month
order by invoice_month;

--number of invoices per month for 2011, using a common table expression
with invoice_per_month as (select date_part(month, cast(invoice_date as Datetime)) as invoice_month,
						   count(distinct invoice) as number_invoices
					from bootcamp.online_transactions ot 
					where date_part(year, cast(invoice_date as Datetime)) = 2011
					group by invoice_month
					order by invoice_month)
select 
	invoice_month,
	number_invoices
from invoice_per_month
;

-- get the name of the month
select invoice_date,
	   to_char(cast(invoice_date as Datetime), 'Month')
from bootcamp.online_transactions ot
where date_part(year, cast(invoice_date as Datetime)) = 2011
limit 100
;


-- using common table expressions to create a table with the invoice, invoice_date, month, year, day and month name
with invoice_data as (select invoice,
					   --substring(invoice_date,6, 2),
					   cast(invoice_date as Datetime) as invoice_date,
					   date_part(month, cast(invoice_date as Datetime)) as invoice_month,
					   date_part(year, cast(invoice_date as Datetime)) as invoice_year,
					   date_part(day, cast(invoice_date as Datetime)) as invoice_day,
					   to_char(cast(invoice_date as Datetime), 'Month') as invoice_month_name
				from bootcamp.online_transactions ot)
select invoice_month_name,
	   count(distinct invoice) as number_invoices
from invoice_data
where invoice_year = 2011
group by invoice_year, invoice_month_name;

-- in an ideal world, our data would be correctly formatted
select  invoice_date,
		date_part(month, invoice_date) as invoice_month,
		date_part(year, invoice_date) as invoice_year,
		date_part(day, invoice_date) as invoice_day
from bootcamp.online_transactions_cleaned otc 
limit 10;

-- playing around with the month using date part and char
select date_part(month, cast(invoice_date as Datetime)) as invoice_month,
	   to_char(cast(invoice_date as Datetime), 'month') as invoice_month_name1,
	   to_char(cast(invoice_date as Datetime), 'Month') as invoice_month_name2,
	   to_char(cast(invoice_date as Datetime), 'MONTH') as invoice_month_name3,
	   to_char(cast(invoice_date as Datetime), 'MON') as invoice_month_name4,
	   to_char(cast(invoice_date as Datetime), 'mon') as invoice_month_name5,
	   to_char(cast(invoice_date as Datetime), 'Mon') as invoice_month_name6
from bootcamp.online_transactions ot 
limit 10;

-- in an ideal world
select date_part(month, invoice_date) as invoice_month,
	   to_char(invoice_date, 'month') as invoice_month_name1,
	   to_char(invoice_date, 'Month') as invoice_month_name2,
	   to_char(invoice_date, 'MONTH') as invoice_month_name3
from bootcamp.online_transactions_cleaned ot 
limit 10;

--
select date_part(year, cast(invoice_date as Datetime)) as invoice_year,
	   to_char(cast(invoice_date as Datetime), 'YYYY') as invoice_year1,
	   cast(to_char(cast(invoice_date as Datetime), 'YYYY') as int) as invoice_year1,
	   to_char(cast(invoice_date as Datetime), 'Y,YYY') as invoice_year2,
	   to_char(cast(invoice_date as Datetime), 'YY') as invoice_year3
from bootcamp.online_transactions ot 
limit 10;

--DAY OF THE WEEK
select invoice_date,
	   to_char(cast(invoice_date as Datetime), 'DD') as invoice_day,
	   to_char(cast(invoice_date as Datetime), 'D') as invoice_dow,
	   to_char(cast(invoice_date as Datetime), 'DAY') as invoice_dow1,
	   to_char(cast(invoice_date as Datetime), 'day') as invoice_dow2,
	   to_char(cast(invoice_date as Datetime), 'Day') as invoice_dow3
from bootcamp.online_transactions ot 
limit 10;

--using then case when function to map the dow to the name 
with invoice_data as (select invoice,
						   invoice_date,
						   to_char(cast(invoice_date as Datetime), 'D') as invoice_dow,
						   to_char(cast(invoice_date as Datetime), 'Month') as invoice_month,
						   to_char(cast(invoice_date as Datetime), 'YYYY') as invoice_year,
						   to_char(cast(invoice_date as Datetime), 'Day') as invoice_dow_check,
						   case when invoice_dow = 1 then 'Sunday'
							  	   when invoice_dow = 2 then 'Monday'
							  	   when invoice_dow = 3 then 'Tuesday'
							  	   when invoice_dow = 4 then 'Wednesday'
							  	   when invoice_dow = 5 then 'Thursday'
							  	   when invoice_dow = 6 then 'Friday'
							  	   when invoice_dow = 7 then 'Saturday'
							  else 'Corrupt'
							  end as invoice_dow_name
					from bootcamp.online_transactions ot)
select invoice_dow_check,
	   count(distinct invoice)
from invoice_data
where invoice_year = 2011
group by invoice_dow_check
;
	   







