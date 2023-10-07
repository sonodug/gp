-- 1. Create external tables based gp.plan, gp.sales tables with PXF

CREATE EXTERNAL TABLE std3_47.plan_ext (
	"date" date,
	region text,
	matdirec int4,
	quantity int4,
	distr_chan int4
)
LOCATION ('pxf://gp.plan?PROFILE=Jdbc&JDBC_DRIVER=org.postgresql.Driver&DB_URL=jdbc:postgresql://192.168.214.212:5432/postgres&USER=intern&PASS=intern') 
ON ALL
FORMAT 'CUSTOM' ( FORMATTER='pxfwritable_import' )
ENCODING 'UTF8';

CREATE EXTERNAL TABLE std3_47.sales_ext (
	"date" date,
	region text,
	material text,
	distr_chan int4,
	quantity int4,
	check_nm text,
	check_pos text
)
LOCATION ('pxf://gp.sales?PROFILE=Jdbc&JDBC_DRIVER=org.postgresql.Driver&DB_URL=jdbc:postgresql://192.168.214.212:5432/postgres&USER=intern&PASS=intern&PARTITION_BY=date:date&RANGE=2021-01-02:2021-07-27&INTERVAL=1:month') 
ON ALL
FORMAT 'CUSTOM' ( FORMATTER='pxfwritable_import' )
ENCODING 'UTF8';

-- 2. Create external tables based CSV local file with GPFDIST

-- price
CREATE EXTERNAL TABLE std3_47.price_ext
(
	material text,
	region varchar(20),
	distr_chan int4,
	price numeric(8, 2)
)
LOCATION (
	'gpfdist://172.16.128.34:8080/price.csv'
)
ON ALL
FORMAT 'CSV' ( DELIMITER ';' NULL '' ESCAPE '"' QUOTE '"')
ENCODING 'UTF8'
SEGMENT REJECT LIMIT 10 ROWS;

-- chanel
CREATE EXTERNAL TABLE std3_47.chanel_ext
(
	material text,
	region varchar(20),
	distr_chan int4,
	price numeric(8, 2)
)
LOCATION (
	'gpfdist://172.16.128.34:8080/chanel.csv'
)
ON ALL
FORMAT 'CSV' ( DELIMITER ';' NULL '' ESCAPE '"' QUOTE '"')
ENCODING 'UTF8'
SEGMENT REJECT LIMIT 5 ROWS;

-- product
CREATE EXTERNAL TABLE std3_47.product_ext
(
	material text,
	region varchar(20),
	distr_chan int4,
	price numeric(8, 2)
)
LOCATION (
	'gpfdist://172.16.128.34:8080/product.csv'
)
ON ALL
FORMAT 'CSV' ( DELIMITER ';' NULL '' ESCAPE '"' QUOTE '"')
ENCODING 'UTF8'
SEGMENT REJECT LIMIT 5 ROWS;

-- region
CREATE EXTERNAL TABLE std3_47.region_ext
(
	material text,
	region varchar(20),
	distr_chan int4,
	price numeric(8, 2)
)
LOCATION (
	'gpfdist://172.16.128.34:8080/region.csv'
)
ON ALL
FORMAT 'CSV' ( DELIMITER ';' NULL '' ESCAPE '"' QUOTE '"')
ENCODING 'UTF8'
SEGMENT REJECT LIMIT 5 ROWS;

DROP EXTERNAL TABLE std3_47.chanel_ext;
DROP EXTERNAL TABLE std3_47.price_ext;
DROP EXTERNAL TABLE std3_47.region_ext;
DROP EXTERNAL TABLE std3_47.product_ext;