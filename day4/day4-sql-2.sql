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
-- 	values(source.emp_id, source.name, source.salary

-- alter table new_employees modify column emp_id int, add primary key(emp_id)

-- insert into new_employees (emp_id, name, salary)
-- select emp_id, name, salary from employees as t
-- on duplicate key update 
-- 	name=t.name,
--     salary=t.salary

-- CREATE TABLE sales (
--   sale_id INT PRIMARY KEY,
--   sale_date DATE,
--   amount DECIMAL(10,2)
-- );

-- INSERT INTO sales VALUES
-- (1, '2024-01-05', 1500),
-- (2, '2024-01-20', 2200),
-- (3, '2024-02-10', 3000),
-- (4, '2024-03-12', 3500),
-- (5, '2024-03-20', 1800);

-- select date_format(sale_date, '%Y-%m'), sum(amount) monthly, sum(amount) over (order by sale_date)
-- from sales
-- group by date_format(sale_date, '%Y-%m')


-- select m.month, m.monthly, sum(m.monthly) over (order by m.month)
-- from (
-- 	select date_format(sale_date, '%Y-%m') month, sum(amount) monthly
--     from sales
--     group by date_format(sale_date, '%Y-%m')
-- ) m


-- select * from accounts

-- update accounts set balance=balance-100 where acc_id=1
-- SET autocommit = 0;

-- start transaction;
-- update accounts set balance=balance+100 where acc_id=1;
-- select sleep(15);
-- update accounts set balance=balance-100 where acc_id=1;
-- commit;


-- create table sample (id int primary key auto_increment, name varchar(100), lastname varchar(100))

delimiter //
create procedure addUser(in p_name varchar(100), in p_lastname varchar(100), out p_new_id int)
begin 
	insert into sample (name, lastname) values (p_name, p_lastname);
    set p_new_id = last_insert_id();
end //
delimiter ;

call addUser('dsfsdf', 'sdf', @new_id);
select @new_id;