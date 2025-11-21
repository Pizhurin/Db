-- Вывести все уникальные бренды, у которых есть хотя бы один продукт со стандартной стоимостью выше 1500 долларов, и суммарными продажами не менее 1000 единиц
select distinct p.brand
from product p
where p.standard_cost > 1500
and (
	select sum(oi.quantity)
	from product p2 
	left join order_items oi on p2.product_id = oi.product_id 
	) > 1000; 



-- Для каждого дня в диапазоне с 2017-04-01 по 2017-04-09 включительно вывести количество подтвержденных онлайн-заказов и количество уникальных клиентов, совершивших эти заказы.
with dates as (
   	select * 
   	from orders o 
   	where o.online_order = true and o.order_status = 'Approved'
   	and o.order_date::date between '2017-04-01' and '2017-04-09'
	)
select d.order_date::date, count(distinct d.order_id), count(distinct d.customer_id)   
from dates d
left join customer c on c.customer_id = d.customer_id	
group by d.order_date::date;

	

-- Вывести профессии клиентов:
-- из сферы IT, чья профессия начинается с Senior;
-- из сферы Financial Services, чья профессия начинается с Lead.
-- Для обеих групп учитывать только клиентов старше 35 лет. Объединить выборки с помощью UNION ALL.

select customer_id, first_name, last_name, job_industry_category, job_title, c."DOB"
from customer c 
where c.job_industry_category = 'IT'
    and c.job_title LIKE 'Senior%'     
union all    
select customer_id, first_name, last_name, job_industry_category, job_title, c."DOB"
from customer c 
where c.job_industry_category = 'Financial Services'
    and c.job_title LIKE 'Lead%';
    
   

-- Вывести бренды, которые были куплены клиентами из сферы Financial Services, но не были куплены клиентами из сферы IT.
with finServicesClient as(
	select * 
	from customer c 
	where c.job_industry_category = 'Financial Services'
	),
orders_id as (
	select * 
	from finServicesClient as fsc
	left join orders o on fsc.customer_id = o.customer_id
	),
product_ids as(
	select oi.product_id, oids.job_industry_category
	from orders_id as oids
	left join order_items oi on oids.order_id = oi.order_id 
)
select distinct p.brand, pids.job_industry_category   
from product p
left join product_ids as pids on p.product_id = pids.product_id;


	
-- Вывести 10 клиентов (ID, имя, фамилия), которые совершили наибольшее количество онлайн-заказов (в штуках) брендов Giant Bicycles, Norco Bicycles, Trek Bicycles, при условии, что они активны и имеют оценку имущества (property_valuation) выше среднего среди клиентов из того же штата.
with state_avg as (
    select c.state, avg(c.property_valuation) as avg_val
    from customer c
    where c.property_valuation is not null
    group by c.state
),
online_orders as (
    select c.customer_id, c.first_name, c.last_name, c.property_valuation, c.state, count(o.order_id) as online_count
    from customer c
    	join orders o on c.customer_id = o.customer_id
    	join order_items oi on o.order_id = oi.order_id
    	join product p on oi.product_id = p.product_id
    where o.online_order = true
      and p.brand in ('Giant Bicycles', 'Norco Bicycles', 'Trek Bicycles')
      and c.property_valuation > (
          select avg_val 
          from state_avg sa 
          where sa.state = c.state
      )
    group by c.customer_id, c.first_name, c.last_name, c.property_valuation, c.state
)
select oo.customer_id, oo.first_name, oo.last_name, oo.online_count
from online_orders oo
order by oo.online_count desc
limit 10;

	
-- Вывести всех клиентов (ID, имя, фамилия), у которых нет подтвержденных онлайн-заказов за последний год, но при этом они владеют автомобилем и их сегмент благосостояния не Mass Customer.
with online_orders_on_last_year as (
    select distinct c.customer_id
    from customer c
        join orders o on c.customer_id = o.customer_id
    where o.online_order = true
      and o.order_status = 'Approved'
      and date_trunc('year', o.order_date::date) = '2017-12-31' 
),
customers_on_last_year as (
    select 
        c.customer_id,
        c.first_name,
        c.last_name         
    from customer c
    where c.owns_car = 'Yes'
      and c.wealth_segment not in ('Mass Customer')
      and c.customer_id not in (
          select customer_id  
          from online_orders_on_last_year
      )
)
select 
    c_last.customer_id,
    c_last.first_name,
    c_last.last_name
from customers_on_last_year c_last;


	
-- Вывести всех клиентов из сферы 'IT' (ID, имя, фамилия), которые купили 2 из 5 продуктов с самой высокой list_price в продуктовой линейке Road.
with road_products as (
    select p.product_id, p.list_price
    from product p
    where product_line = 'Road'
),
customers_orders as (
    select distinct c.customer_id, c.first_name, c.last_name, oi.product_id
    from customer c
        join orders o on c.customer_id = o.customer_id
        join order_items oi on o.order_id = oi.order_id
    where c.job_industry_category = 'IT'
      and oi.product_id in 
      (
      select product_id from road_products
      )
)
select co.customer_id, co.first_name, co.last_name
from customers_orders co
group by customer_id, first_name, last_name
having count(distinct co.product_id) >= 2 and count(distinct co.product_id) <= 5;

	
-- Вывести клиентов (ID, имя, фамилия, сфера деятельности) из сфер IT или Health, которые совершили не менее 3 подтвержденных заказов в период 2017-01-01 по 2017-03-01, и при этом их общий доход от этих заказов превышает 10 000 долларов.
-- Разделить вывод на две группы (IT и Health) с помощью UNION.
with customer_orders as (
    select c.customer_id, c.first_name, c.last_name, c.job_industry_category,
        count(o.order_id) as count,
        sum(oi.quantity * oi.item_list_price_at_sale) as total
    from customer c
        join orders o on c.customer_id = o.customer_id
        join order_items oi on o.order_id = oi.order_id
    where c.job_industry_category in ('IT', 'Health')
      and o.order_status = 'Approved'
      and o.order_date between '2017-01-01' and '2017-03-01'
    group by  c.customer_id, c.first_name, c.last_name, c.job_industry_category
    having 
        count(o.order_id) >= 3
        and sum(oi.quantity * oi.item_list_price_at_sale) > 10000
)
select *
from (
	select co.customer_id, co.first_name, co.last_name, co.job_industry_category
	from customer_orders co
	where job_industry_category = 'IT'
union
	select co2.customer_id, co2.first_name, co2.last_name, co2.job_industry_category
	from customer_orders co2
	where job_industry_category = 'Health'
) as res
order by res.job_industry_category
	


 