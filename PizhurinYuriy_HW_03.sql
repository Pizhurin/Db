-- Вывести распределение (количество) клиентов по сферам деятельности, отсортировав результат по убыванию количества.
select 
	c.job_industry_category,
	count(c.customer_id) as count_customers
from customer c 
group by c.job_industry_category 
order by count(c.customer_id) desc;


-- Найти общую сумму дохода (list_price*quantity) по всем подтвержденным заказам за каждый месяц по сферам деятельности клиентов. 
-- Отсортировать результат по году, месяцу и сфере деятельности.
select 
	date_part('year', o.order_date::date) as year,
    date_part('month', o.order_date::date) as month,
    c.job_industry_category,
	sum(oi.quantity * p.list_price) as total_sum
from order_items oi 
	left join product p on coalesce(oi.product_id, 0) = coalesce(p.product_id, 0)
	left join orders o on  coalesce(oi.order_id , 0) = coalesce(o.order_id)
	left join customer c on c.customer_id  = o.customer_id 
where o.order_status = 'Approved'
group by 
	date_part('year', o.order_date::date),
    date_part('month', o.order_date::date),
    c.job_industry_category
order by 
	date_part('year', o.order_date::date),
    date_part('month', o.order_date::date),
    c.job_industry_category;


-- Вывести количество уникальных онлайн-заказов для всех брендов в рамках подтвержденных заказов клиентов из сферы IT. 
-- Включить бренды, у которых нет онлайн-заказов от IT-клиентов, — для них должно быть указано количество 0.
with brands as (
    select distinct p.brand 
    from product p
    where p.brand is not null
),
online_orders_it as (
    select 
        p.brand,
        count(distinct o.order_id) as order_count
    from orders o
        join customer c on o.customer_id = c.customer_id
        join order_items oi on o.order_id = oi.order_id
        join product p on oi.product_id = p.product_id
    where c.job_industry_category = 'IT'
      and o.online_order = true
      and o.order_status = 'Approved'
    group by p.brand
)
select 
    b.brand,
    coalesce(oit.order_count, 0) as online_order_count
from brands b
left join online_orders_it oit on b.brand = oit.brand
order by b.brand;



-- Найти по всем клиентам: сумму всех заказов (общего дохода), максимум, минимум и количество заказов, а также среднюю сумму заказа по каждому клиенту. 
-- Отсортировать результат по убыванию суммы всех заказов и количества заказов. Выполнить двумя способами: используя только GROUP BY и используя только оконные функции. 
-- Сравнить результат.
-- Через Group By
select 
    c.customer_id,
    c.first_name,
    c.last_name,
    sum(oi.quantity * oi.item_list_price_at_sale) as sum_sales,
    max(oi.quantity * oi.item_list_price_at_sale) as max_sales,
    min(oi.quantity * oi.item_list_price_at_sale) as min_sales,
    count(distinct o.order_id) as count_ordes,
    avg(oi.quantity * oi.item_list_price_at_sale) as avg_sales
from customer c
    left join orders o on c.customer_id = o.customer_id
    left join order_items oi on o.order_id = oi.order_id
group by 
    c.customer_id,
    c.first_name,
    c.last_name
    order by sum_sales, count_ordes
    
-- Через Window  
    

-- Найти имена и фамилии клиентов с топ-3 минимальной и топ-3 максимальной суммой транзакций за весь период (учесть клиентов, у которых нет заказов, 
-- приняв их сумму транзакций за 0).
with customer_total_amount as (
    select 
        c.customer_id,
        c.first_name,
        c.last_name,
        coalesce(sum(oi.quantity * oi.item_list_price_at_sale), 0) as total_amount
    from customer c
        left join orders o on c.customer_id = o.customer_id
        left join order_items oi on o.order_id = oi.order_id
    group by 
    	c.customer_id, 
    	c.first_name, 
    	c.last_name
),
customers_rank as (
    select 
        customer_id,
        first_name,
        last_name,
        total_amount,
        dense_rank() over (order by total_amount asc) as min_drnk,
        dense_rank() over (order by total_amount desc) as max_drnk,
        row_number() over (order by total_amount asc) as min_rn,
        row_number() over (order by total_amount desc) as max_rn
    from customer_total_amount
)
select 
    first_name,
    last_name,
    total_amount
from customers_rank
where min_rn <= 3
union all
select 
    first_name,
    last_name,
    total_amount
from customers_rank
where max_rn <= 3
order by total_amount;
    

-- Вывести только вторые транзакции клиентов (если они есть) с помощью оконных функций. Если у клиента меньше двух транзакций, он не должен попасть в результат.
with numbered_orders as (
    select 
        c.customer_id,
        c.first_name,
        c.last_name,
        o.order_id,
        o.order_date,
        sum(oi.quantity * oi.item_list_price_at_sale) as sum_sales,
        row_number() over (partition by c.customer_id order by o.order_date, o.order_id) as rn
    from customer c
        join orders o on c.customer_id = o.customer_id
        join order_items oi on o.order_id = oi.order_id
    group by 
        c.customer_id,
        c.first_name,
        c.last_name,
        o.order_id,
        o.order_date
)
select 
    customer_id,
    first_name,
    last_name,
    order_id,
    order_date,
    sum_sales
from numbered_orders
where rn = 2;

-- Вывести имена, фамилии и профессии клиентов, а также длительность максимального интервала (в днях) между двумя последовательными заказами. 
-- Исключить клиентов, у которых только один или меньше заказов.
with customer_order_date as (
    select 
        c.customer_id,
        c.first_name,
        c.last_name,
        c.job_title,
        o.order_id,
        o.order_date::date as order_date
    from customer c
        join orders o on c.customer_id = o.customer_id
),
interval_between_orders as (
    select 
        customer_id,
        first_name,
        last_name,
        job_title,
        order_date,
        lead(order_date) over (partition by customer_id order by order_date) as lead_order_date,
        (lead(order_date) over (partition by customer_id order by order_date) - order_date) as days_between_orders
    from customer_order_date
),
max_interval_day as (
    select 
        customer_id,
        first_name,
        last_name,
        job_title,
        max(days_between_orders) as max_interval
    from interval_between_orders
    where days_between_orders is not null
    group by 
        customer_id,
        first_name,
        last_name,
        job_title
)
select 
    first_name,
    last_name,
    job_title,
    max_interval
from max_interval_day
order by max_interval desc;


-- Найти топ-5 клиентов (по общему доходу) в каждом сегменте благосостояния (wealth_segment). 
-- Вывести имя, фамилию, сегмент и общий доход. Если в сегменте менее 5 клиентов, вывести всех.
with customer_sum_sales as (
    select 
        c.customer_id,
        c.first_name,
        c.last_name,
        c.wealth_segment,
        coalesce(sum(oi.quantity * oi.item_list_price_at_sale), 0) as sum_sales
    from customer c
        left join orders o on c.customer_id = o.customer_id
        left join order_items oi on o.order_id = oi.order_id
    group by 
        c.customer_id,
        c.first_name,
        c.last_name,
        c.wealth_segment
),
customers_rn_segment as (
    select 
        customer_id,
        first_name,
        last_name,
        wealth_segment,
        sum_sales,
        row_number() over (partition by wealth_segment order by sum_sales desc) as rn_segment
    from customer_sum_sales
)
select 
    first_name,
    last_name,
    wealth_segment,
    sum_sales
from customers_rn_segment
where rn_segment <= 5
order by 
    wealth_segment,
    rn_segment;





