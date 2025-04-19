USE DEC03Assignment;






-- Unoptimized query written by AI

explain analyze
WITH
  -- 1. First, grab your sales filter but cast every column to VARCHAR
  sales_step AS (
    SELECT
      CAST(s0.salesid      AS VARCHAR) AS salesid_str,
      CAST(s0.customerid   AS VARCHAR) AS customerid_str,
      CAST(s0.productid    AS VARCHAR) AS productid_str,
      s0.*
    FROM (
      SELECT * 
      FROM sales
      WHERE discount > 0 
         OR quantity > 10
    ) AS s0
  ),

  -- 2. Attach customer names via a sub‑subquery
  cust_step AS (
    SELECT ss.*, c0.customer_name
    FROM sales_step ss
    JOIN (
      SELECT customerid, firstname || ' ' || lastname AS customer_name
      FROM customers
    ) AS c0
      ON ss.customerid = c0.customerid
  ),

  -- 3. Attach product names via another sub‑subquery
  prod_step AS (
    SELECT cs.*, p0.product_name
    FROM cust_step cs
    JOIN (
      SELECT productid, productname AS product_name
      FROM products
    ) AS p0
      ON cs.productid = p0.productid
  ),

  -- 4. Identify zero‑price customers
  zero_price_cust AS (
    SELECT DISTINCT customerid
    FROM (
      SELECT * 
      FROM sales
      WHERE totalprice = 0
    ) AS zp
  ),

  -- 5. Filter by those customers
  filter_cust AS (
    SELECT ps.*
    FROM prod_step ps
    WHERE ps.customerid IN (SELECT customerid FROM zero_price_cust)
  ),

  -- 6. Identify recent sales
  recent_sales AS (
    SELECT salesid
    FROM (
      SELECT * FROM sales
    ) AS all_s
    WHERE all_s.salesdate > '2018-01-01'
  ),

  -- 7. Filter again by recent sales
  filter_both AS (
    SELECT fc.*
    FROM filter_cust fc
    WHERE fc.salesid IN (SELECT salesid FROM recent_sales)
  ),

  -- 8. Number every row (but keep them all)
  numbered AS (
    SELECT 
      fb.*, 
      ROW_NUMBER() OVER (PARTITION BY fb.customerid ORDER BY fb.salesid) AS rn
    FROM filter_both fb
  ),

  -- 9. Dummy UNION to bloat things further
  final_union AS (
    SELECT * FROM numbered
    UNION ALL
    SELECT * FROM numbered WHERE 1=0
  )

-- 10. Final select with a CROSS JOIN LATERAL that does nothing but count
SELECT
  fu.*
FROM final_union fu
CROSS JOIN LATERAL (
  SELECT COUNT(*) AS total_count
  FROM final_union
) AS cnt
WHERE fu.rn >= 1    -- keeps all rows
ORDER BY
  fu.salesdate DESC,
  fu.customerid,
  fu.salesid;




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
  where totalprice = 0
)

 select
  s.salesid,
  s.customerid,
  s.productid,
  c.firstname || ' ' || c.lastname as customer_name,
  p.productname as product_name
from sales s
join zero_price_customers zpc on s.customerid = zpc.customerid
join customers c on s.customerid = c.customerid
join products p on s.productid = p.productid
where s.salesdate > '2018-01-01'
  and (s.discount > 0 or s.quantity > 10);





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