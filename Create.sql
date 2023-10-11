-- CREATING FACT TABLES

-- OPTIONAL
drop table if exists std3_47.plan;
drop table if exists std3_47.sales;
--

create table std3_47.plan
(
	"date" date NOT NULL,
	region text NOT NULL,
	matdirec text NOT NULL,
	quantity int4 NOT NULL,
	distr_chan int4 NOT NULL
)
WITH (
	appendonly=true,
	orientation=column,
	compresstype=zstd,
	compresslevel=1
)
distributed by (date);

-- insert throw csv (not working), use import
\copy std3_47.plan from '<\Users\maks_\OneDrive\Desktop\Sapinens\plan.csv>' delimiter ';' CSV HEADER;

--check
select * from std3_47.plan;

--check skcoeff for table plan
select gp_segment_id, count(*)
from std3_47.plan
group by 1
order by gp_segment_id;

select (gp_toolkit.gp_skew_coefficients('std3_47.plan'::regclass)).skccoeff;

-- created a temp table sales
create table std3_47.tempsales
(
	"date" date NULL,
	region text NULL,
	material text NULL,
	distr_chan int4 NULL,
	quantity int4 null,
	check_nm text not null,
	check_pos text not null
)
with (
	appendonly=true,
	orientation=row,
	compresstype=zstd,
	compresslevel=1
)
distributed by (check_pos);

-- check min_date and max_date before partition
SELECT min(date) AS min_date, max(date) AS max_date
FROM std3_47.tempsales;

create table std3_47.sales
(
	"date" date NULL,
	region text NULL,
	material text NULL,
	distr_chan int4 NULL,
	quantity int4 null,
	check_nm text not null,
	check_pos text not null
)
with (
	appendonly=true,
	orientation=row,
	compresstype=zstd,
	compresslevel=1
)
distributed by (check_pos)
partition by range(date)
(
	start (date '2021-01-02') inclusive
	end (date '2021-07-27') exclusive
	every (interval '1 month')
);

insert into std3_47.sales
select * from std3_47.tempsales;

drop table std3_47.tempsales;

--check partition
select
	partitiontablename,
	partitionrangestart,
	partitionrangeend
from pg_partitions
where
	tablename = 'sales'
	and schemaname = 'std3_47'
order by partitionrangestart;

-- CREATING REFERENCE TABLES

--product
CREATE TABLE std3_47.product (
	material varchar(20) NULL,
	asgrp varchar(10) NULL,
	brand varchar(20) NULL,
	matcateg varchar(5) NULL,
	matdirec int4 NULL,
	txt text NULL
)
DISTRIBUTED REPLICATED;

--region
CREATE TABLE std3_47.region (
	region varchar(20) NULL,
	txt text NULL
)
WITH (
	appendonly=true,
	orientation=column,
	compresstype=zstd,
	compresslevel=1
)
DISTRIBUTED REPLICATED;

--chanel
CREATE TABLE std3_47.chanel (
	distr_chan int4 NULL,
	txtsh text NULL
)
WITH (
	appendonly=true,
	orientation=column,
	compresstype=zstd,
	compresslevel=1
)
DISTRIBUTED REPLICATED;

--price
CREATE TABLE std3_47.price (
	material text NULL,
	region varchar(20) NULL,
	distr_chan int4 NULL,
	price numeric(8, 2) NULL
)
WITH (
	appendonly=true,
	orientation=column,
	compresstype=zstd,
	compresslevel=1
)
DISTRIBUTED REPLICATED;


--Check table size
select pg_size_pretty(pg_total_relation_size('std3_47.sales')) as size;