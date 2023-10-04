-- 1. INSERT

create table std00.lineitem (
	-- Definition
)
with (
	appendonly=true,
	orientation=column,
	compresstype=zstd,
	compresslevel=1
)
distributed by (l_partkey);

insert into std00.lineitem (--column_names)
values (--values)

-- 2. COPY (Manually)

-- 3. PXF
	-- 3.1. READABLE
create external table std00.lineitem_ext
(
	-- Definition
)
location ('pxf://dl.lineitem?PROFILE=Jdbc&JDBC_DRIVER=org.postgresql.Driver&DB_URL=jdbc:postgresql://IP:PORT/postgres&USER=user&PASS=password')
on all
format 'CUSTOM' ( FORMATTER='pxfwritable_import' )
encoding 'utf8';

-- With partition
create external table std00.lineitem_ext
(
	-- Definition
)
location ('pxf://dl.lineitem?PROFILE=Jdbc&JDBC_DRIVER=org.postgresql.Driver&DB_URL=jdbc:postgresql://IP:PORT/postgres&USER=user&PASS=password&PARTITION_BY=invoicedate:date&RANGE=2021-01-01:2023-05-01&INTERVAL=106:day'
) on all
format 'CUSTOM' ( FORMATTER='pxfwritable_import' )
encoding 'utf8';

-- 3.2. WRITABLE
CREATE WRITABLE EXTERNAL TABLE std00.lineitem_write_ext 
(
	-- Definition
)
LOCATION ('pxf://dl.lineitem?PROFILE=Jdbc&JDBC_DRIVER=org.postgresql.Driver&DB_URL=jdbc:postgresql://IP:PORT/postgres&USER=user&PASS=password')
ON ALL
FORMAT 'CUSTOM' ( FORMATTER='pxfwritable_export' )
ENCODING 'utf8';

-- 4. GPFDIST

-- READABLE

CREATE EXTERNAL TABLE std00.lineitem_gpfdist_ext
(
	-- Definition
)
LOCATION (
	'gpfdist://IP:PORT_local/lineitem*.csv'
)
ON ALL
FORMAT 'CSV' ( DELIMITER ':' NULL '' ESCAPE '"' QUOTE '"')
ENCODING 'UTF8'
SEGMENT REJECT LIMIT 10 ROWS;

-- WRITABLE

CREATE WRITABLE EXTERNAL TABLE std00.lineitem_gpfdist_write_ext (LIKE adb.std00.lineitem)
LOCATION (
	'gpfdist://IP:PORT_local/lineitem_out.csv'
)
FORMAT 'CSV' ( DELIMITER ':' NULL '' ESCAPE '"' QUOTE '"')
DISTRIBUTED BY (l_partkey);


