-- select emp_id, name, salary, department, rank() over (partition by department order by salary) as salary_rank
-- from employees

-- select department, avg(salary)
-- from employees
-- group by department

-- with rt as (select name, department, salary, dense_rank() over (partition by department order by salary desc) as rnk
-- from employees
-- )

-- select name, department, salary
-- from rt
-- where rnk <4

-- SELECT * FROM day4.customers;
-- alter table customers add column cust_id int unique auto_increment;
-- alter table customers add column registered_on timestamp
-- SELECT * FROM day4.orders;
-- alter table orders add column order_date timestamp

-- select o.order_id, o.amount, c.cust_id, c.registered_on, o.order_date, timestampdiff(month, o.order_date, current_timestamp) months_from_last_order
-- from orders o join customers c on c.cust_id=o.cust_id
-- where o.order_date < current_timestamp - interval 1 year

-- select emp_id, department, salary, sum(salary) over (partition by department) as total_salary
-- from employees

-- select e.emp_id, d.avgS
-- from employees e
-- join (select department, avg(salary) as avgS from employees group by department) d on e.department = d.department

-- select name as unionname
-- from (select name from customers
-- union
-- select name from suppliers) as final

-- select name from customers
-- except
-- select name from suppliers

-- create view summary as 
-- select region, sum(amount) total_sales
-- from orders
-- group by region

-- create table new_empl(
-- 	emp_id int primary key auto_increment,
--     name varchar(50),
--     salary int
-- )

-- merge into employees as target
-- using new_employees as source
-- on target.emp_id = source.emp_id
-- when matched then
-- 	update set salary = source.salary
-- when not matched then
-- 	insert(emp_id, name, salary)
-- 	values(source.emp_id, source.name, source.salary)


