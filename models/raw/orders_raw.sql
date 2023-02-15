
select * from dbt_demo_project.raw.orders_raw 
where o_custkey in (1,2,5)
order by o_orderkey
