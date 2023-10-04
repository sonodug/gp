-- 1 TASK
-- Partitions selected: 168 (out of 168)
-- Execution time: 140.208 ms
explain analyze SELECT SUM(re.sale_price)
FROM homework_3.real_estate re 
WHERE date_trunc('year',"date") = '2009-01-01';

SELECT SUM(re.sale_price)
FROM homework_3.real_estate re 
WHERE date_trunc('year',"date") = '2009-01-01';

-- Partitions selected: 12 (out of 168)
-- Execution time: 20 ms
explain analyze SELECT SUM(re.sale_price)
FROM homework_3.real_estate re 
WHERE "date" >= '2009-01-01' AND "date" <= '2009-12-31';

SELECT SUM(re.sale_price)
FROM homework_3.real_estate re 
WHERE "date" >= '2009-01-01' AND "date" <= '2009-12-31';

-- Optional
select * from homework_3.real_estate
WHERE date_trunc('year',"date") = '2009-01-01';

select count(*)
from homework_3.real_estate re;

select
	partitiontablename,
	partitionrangestart,
	partitionrangeend
from pg_partitions
where
	tablename = 'homework_3'
	and schemaname = 'real_estate'
order by partitionrangestart;

-- 2 TASK
-- 2.1
SELECT *
FROM homework_3.inventory_parts ip
WHERE ip.inventory_id IN (SELECT DISTINCT id FROM homework_3.colors c);

explain analyze SELECT *
FROM homework_3.inventory_parts ip
WHERE ip.inventory_id IN (SELECT DISTINCT id FROM homework_3.colors c);

SELECT inventory_id, part_num, color_id, quantity, is_spare
FROM homework_3.inventory_parts ip
join (
    SELECT DISTINCT id
    FROM homework_3.colors
) c ON ip.inventory_id = c.id;

ANALYZE homework_3.inventory_parts;
ANALYZE homework_3.colors;

explain analyze select inventory_id, part_num, color_id, quantity, is_spare
FROM homework_3.inventory_parts ip
join (
    SELECT DISTINCT id
    FROM homework_3.colors
) c ON ip.inventory_id = c.id;

-- 2.2
SELECT c.id, SUM(ip.quantity)
FROM homework_3.colors c
JOIN (SELECT ip.inventory_id,
             ip.part_num,
             ip.color_id,
             ip.quantity,
             ip.is_spare
       FROM homework_3.inventory_parts ip) ip
ON c.id = ip.color_id
GROUP BY c.id;

explain analyze SELECT c.id, SUM(ip.quantity)
FROM homework_3.colors c
JOIN (SELECT ip.inventory_id,
             ip.part_num,
             ip.color_id,
             ip.quantity,
             ip.is_spare
       FROM homework_3.inventory_parts ip) ip
ON c.id = ip.color_id
GROUP BY c.id;

SELECT c.id, SUM(ip.quantity)
FROM homework_3.colors c
JOIN (SELECT ip.color_id, ip.quantity
       FROM homework_3.inventory_parts ip) ip
ON c.id = ip.color_id
GROUP BY c.id;

explain analyze SELECT c.id, SUM(ip.quantity)
FROM homework_3.colors c
JOIN homework_3.inventory_parts ip
ON c.id = ip.color_id
GROUP BY c.id;

explain analyze SELECT c.id, SUM(ip.quantity)
FROM homework_3.colors c, homework_3.inventory_parts ip
where c.id = ip.color_id
GROUP BY c.id;

explain analyze select c.id, sum(ip.quantity)
from homework_3.inventory_parts ip
join (select c.id from homework_3.colors c) c
on ip.color_id = c.id
group by c.id;

-- 2.3
SELECT ip.inventory_id::int4, 
       c."name":: bpchar(250),
       ip.is_spare::bpchar(1), 
       SUM(ip.quantity)
FROM homework_3.inventory_parts ip 
LEFT JOIN homework_3.colors c 
ON ip.color_id = c.id 
GROUP BY ip.inventory_id, c."name", ip.is_spare;

explain analyze SELECT ip.inventory_id::int4, 
       c."name":: bpchar(250),
       ip.is_spare::bpchar(1), 
       SUM(ip.quantity)
FROM homework_3.inventory_parts ip 
LEFT JOIN homework_3.colors c 
ON ip.color_id = c.id 
GROUP BY ip.inventory_id, c."name", ip.is_spare;

explain analyze SELECT ip.inventory_id, c.name::text, ip.is_spare::text, SUM(ip.quantity)
FROM homework_3.inventory_parts ip 
LEFT JOIN homework_3.colors c 
ON ip.color_id = c.id
GROUP BY ip.inventory_id, c.name, ip.is_spare;