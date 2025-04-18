USE DEC03Assignment;



-- Unoptimized query written by AI

explain analyze
SELECT *
FROM (
  SELECT *
  FROM (
    SELECT
      s0.salesid,
      s0.customerid,
      s0.productid,
      c0.customer_name,
      p0.product_name
    FROM (
      SELECT *
      FROM sales
      WHERE discount > 0
         OR quantity > 10
    ) AS s0

    JOIN (
      SELECT
        customerid,
        firstname || ' ' || lastname AS customer_name
      FROM customers
    ) AS c0
      ON s0.customerid = c0.customerid

    JOIN (

      SELECT
        productid,
        productname AS product_name
      FROM products
    ) AS p0
      ON s0.productid = p0.productid

  ) AS s1

  WHERE

    s1.customerid IN (
      SELECT s2.customerid
      FROM (
        SELECT DISTINCT s3.customerid
        FROM (
          SELECT *
          FROM sales
          WHERE totalprice = 0
        ) AS s3
      ) AS s2
    )

    AND

    s1.salesid IN (
      SELECT s4.salesid
      FROM (
        SELECT *
        FROM sales
      ) AS s4
      WHERE s4.salesdate > '2018-01-01'
    )

) AS final_sub;





-- Optimized query

drop index idx_discount;

drop index idx_quantity;

drop index idx_salesdate;


create index idx_discount on sales(discount);

create index idx_quantity on sales(quantity);

create index idx_salesdate on sales(salesdate);


explain analyze
with zero_price_customers as (
    select distinct customerid
    from sales
    where totalprice = 0),

  filtered_sales as (
    select salesid, customerid, productid
    from sales
    where salesdate > '2018-01-01' and (discount > 0 or quantity > 10))
    
select *
from filtered_sales fs
join zero_price_customers zpc on fs.customerid = zpc.customerid
join customers c on fs.customerid = c.customerid
join products p on fs.productid = p.productid;





-- Additional: Query optimization in DuckDB 

-- Original script

SELECT count(*)
FROM (
  SELECT type, repo.name, actor.login,
    JSON_EXTRACT_STRING(payload, '$.action') as event
  FROM read_json_auto('https://data.gharchive.org/2025-02-14-12.json.gz')
  )
  WHERE event = 'opened'
  AND TYPE = 'IssuesEvent'
  ;
  
explain analyze 
SELECT count(*)
FROM (
  SELECT type, repo.name, actor.login,
    JSON_EXTRACT_STRING(payload, '$.action') as event
  FROM read_json_auto('https://data.gharchive.org/2025-02-14-12.json.gz')
  )
  WHERE event = 'closed'
  AND TYPE = 'IssuesEvent'
  ;
  
 SELECT count(*)
FROM (
  SELECT type, repo.name, actor.login,
    JSON_EXTRACT_STRING(payload, '$.action') as event
  FROM read_json_auto('https://data.gharchive.org/2025-02-14-12.json.gz')
  )
  WHERE event = 'reopened'
  AND TYPE = 'IssuesEvent'
  ;
  
 
-- Optimized query

explain analyze
with parsed as (
  select
    type,
    json_extract_string(payload, '$.action') as event
  from read_json_auto('https://data.gharchive.org/2025-02-14-12.json.gz')
  where type = 'IssuesEvent'
)

select
  sum(case when event = 'reopened' then 1 else 0 end) as reopened
from parsed;