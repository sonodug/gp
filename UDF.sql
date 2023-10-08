CREATE OR REPLACE FUNCTION std3_47.f_load_full(p_table TEXT, p_file_name text)
	RETURNS int4
	LANGUAGE plpgsql
	VOLATILE
AS $$
DECLARE
	v_ext_table_name TEXT;
	v_sql TEXT;
	v_gpfdist TEXT;
	v_result int;
BEGIN
	v_ext_table_name = p_table||'_ext';
	EXECUTE 'TRUNCATE TABLE '||p_table;
	EXECUTE 'DROP EXTERNAL TABLE IF EXISTS '||v_ext_table_name;

	v_gpfdist = 'gpfdist://172.16.128.34:8080/'||p_file_name||'.csv';

	v_sql = 'CREATE EXTERNAL TABLE '||v_ext_table_name||'(LIKE '||p_table||')
			 LOCATION ('''||v_gpfdist||'''
			 ) ON ALL
			 FORMAT ''CSV'' ( HEADER DELIMITER '';'' NULL '''' ESCAPE ''"'' QUOTE ''"'' )
			 ENCODING ''UTF8''';
	
	RAISE NOTICE 'EXTERNAL TABLE IS: %', v_sql;
	EXECUTE v_sql;
	EXECUTE 'INSERT INTO '||p_table||' SELECT * FROM '||v_ext_table_name;
	EXECUTE 'SELECT COUNT(1) FROM '||p_table INTO v_result;
	RETURN v_result;
END;
$$
EXECUTE ON ANY;

--
CREATE OR REPLACE FUNCTION std3_47.f_load_simple_partition(p_table TEXT, p_partition_key TEXT,
														   p_start_date timestamp, p_end_date timestamp,
														   p_pxf_table TEXT, p_user_id TEXT, p_pass TEXT)
	RETURNS int4
	LANGUAGE plpgsql
	VOLATILE
AS $$
DECLARE
	v_ext_table TEXT;
	v_temp_table TEXT;
	v_sql TEXT;
	v_pxf TEXT;
	v_result TEXT;
	v_dist_key TEXT;
	v_params TEXT;
	v_where TEXT;
	v_load_interval INTERVAL;
	v_start_date date;
	v_end_date date;
	v_table_oid int4;
	v_cnt int8;
BEGIN
	v_ext_table = p_table||'_ext';
	v_temp_table = p_table||'_tmp';

	SELECT c.oid
	INTO v_table_oid
	FROM pg_class AS c INNER JOIN pg_namespace AS n ON c.relnamespace = n.oid
	WHERE n.nspname||'.'||c.relname = p_table
	LIMIT 1;

	IF v_table_oid = 0 OR v_table_oid IS NULL THEN
		v_dist_key = 'DISTRIBUTED RANDOMLY';
	ELSE
		v_dist_key = pg_get_table_distributedby(v_table_oid);
	END IF;

	SELECT COALESCE('with (' || array_to_string(reloptions, ', ') || ')', '')
	FROM pg_class
	INTO v_params
	WHERE oid = p_table::regclass;

	EXECUTE 'TRUNCATE TABLE '||p_table;
	EXECUTE 'DROP EXTERNAL TABLE IF EXISTS '||v_ext_table;

	v_load_interval = '1 month'::INTERVAL;
	-- v_start_date := date_trunc('month', p_start_date);
	-- v_end_date := date_trunc('month', p_start_date) + v_load_interval;

	v_start_date := p_start_date;
	v_end_date := p_start_date + v_load_interval;

	v_where = p_partition_key ||' >= '''||v_start_date||'''::date AND '||p_partition_key||' < '''||v_end_date||'''::date';
	
	v_pxf = 'pxf://'||p_pxf_table||'?PROFILE=Jdbc&JDBC_DRIVER=org.postgresql.Driver&DB_URL=jdbc:postgresql://192.168.214.212:5432/postgres&USER='||p_user_id||'&PASS='||p_pass;
	
	RAISE NOTICE 'PXF CONNECTION STRING: %', v_pxf;

	v_sql = 'CREATE EXTERNAL TABLE '||v_ext_table||'(LIKE '||p_table||')
			 LOCATION ('''||v_pxf||'''
			 ) ON ALL
			 FORMAT ''CUSTOM'' (FORMATTER=''pxfwritable_import'')
			 ENCODING ''UTF8''';
			
	RAISE NOTICE 'EXTERNAL TABLE IS: %', v_sql;

	EXECUTE v_sql;

	v_sql := 'DROP TABLE IF EXISTS '|| v_temp_table ||';
			  CREATE TABLE '|| v_temp_table ||' (LIKE '||p_table||') ' ||v_params||' '||v_dist_key||';';
			 
	RAISE NOTICE 'TEMP TABLE IS: %', v_sql;
	EXECUTE v_sql;

	v_sql = 'INSERT INTO '||v_temp_table||' SELECT * FROM '||v_ext_table||' WHERE '||v_where;
	EXECUTE v_sql;

	v_sql = 'INSERT INTO '||v_temp_table||' SELECT * FROM '||v_ext_table||' WHERE '||v_where;
	EXECUTE v_sql;

	GET DIAGNOSTICS v_cnt = ROW_COUNT;
	RAISE NOTICE 'INSERTED ROWS: %', v_cnt;

	v_sql = 'ALTER TABLE '||p_table||' EXCHANGE PARTITION FOR (DATE '''||v_start_date||''') WITH TABLE '|| v_temp_table ||' WITH VALIDATION';
	
	RAISE NOTICE 'EXCHANGE PARTITION SCRIPT: %', v_sql;
	EXECUTE v_sql;
	
	EXECUTE 'SELECT COUNT(1) FROM '||p_table||' WHERE '||v_where INTO v_result;
	
	RETURN v_result;
END;
$$
EXECUTE ON ANY;
--
CREATE OR REPLACE FUNCTION std3_47.f_load_write_log(p_log_type TEXT, p_log_message TEXT, p_location TEXT)
	RETURNS void
	LANGUAGE plpgsql
	VOLATILE
AS $$
DECLARE
	v_log_type TEXT;
	v_log_message TEXT;
	v_sql TEXT;
	v_location TEXT;
	v_res TEXT;
BEGIN
	v_log_type = upper(p_log_type);
	v_location = lower(p_location);
	IF v_log_type NOT IN ('ERROR', 'INFO') THEN
		RAISE EXCEPTION 'Illegal log type. Use one of: ERROR, INFO';
	END IF;

	RAISE NOTICE '%: %: <%> Location[%]', clock_timestamp(), v_log_type, p_log_messaage, v_location;
	
	v_log_message := replace(p_log_message, '''', '''''');
	
	v_sql := 'INSERT INTO std3_47.logs(log_id, log_type, log_msg, log_location, is_error, log_timestamp, log_user)
			  VALUES ( ' || nextval('std3_47.log_id_seq')|| ',
					 ''' || v_log_type || ''',
					   ' || COALESCE('''' || v_log_message || '''', '''empty''')|| ',
					   ' || COALESCE('''' || v_location || '''', 'null')|| ',
					   ' || CASE WHEN v_log_type = 'ERROR' THEN TRUE ELSE FALSE END|| ',
						 current_timestamp, current_user);';
						
	RAISE NOTICE 'INSERT SQL IS: %', v_sql;
	v_res := dblink('adb_server', v_sql);
END;
$$
EXECUTE ON ANY;
--
CREATE OR REPLACE FUNCTION std3_47.f_load_mart(p_month varchar)
	RETURNS int4
	LANGUAGE plpgsql
	VOLATILE
AS $$
DECLARE
	v_table_name TEXT;
	v_sql TEXT;
	v_return int;
BEGIN
	PERFORM std3_47.f_load_write_log(p_log_type := 'INFO', p_log_message := 'Start f_load_mart', p_location := 'Sales mart calculation');

	DROP TABLE IF EXISTS std3_47.mart;
	CREATE TABLE std3_47.mart
	WITH (
		appendonly=TRUE,
		orientation=COLUMN,
		compresstype=zstd,
		compresslevel=1 )
	AS SELECT region, material, distr_chan, sum(quantity) qnt, count(DISTINCT check_nm) chk_cnt
	FROM std3_47.sales
	WHERE date BETWEEN date_trunc('month', to_date(p_month, 'YYYYMM')) - INTERVAL'3 month'
		AND date_trunc('month', to_date(p_month, 'YYYYMM'))
	GROUP BY 1,2,3
	DISTRIBUTED BY (material);

	SELECT count(*) INTO v_return FROM std3_47.mart;

	PERFORM std3_47.f_load_write_log(p_log_type := 'INFO', p_log_message := v_return || ' rows inserted', p_location := 'Sales mart calculation');
	PERFORM std3_47.f_load_write_log(p_log_type := 'INFO', p_log_message := 'End f_load_mart', p_location := 'Sales mart calculation');

	RETURN v_return;
END;
$$
EXECUTE ON ANY;
--
CREATE OR REPLACE FUNCTION std3_47.f_calculate_plan_mart(p_month varchar)
	RETURNS int4
	LANGUAGE plpgsql
	VOLATILE
AS $$
DECLARE
	v_table_name text;
	v_sql text;
	v_return text;
	v_load_interval interval;
	v_start_date date;
	v_end_date date;
BEGIN
	PERFORM std3_47.f_load_write_log(p_log_type := 'INFO',
									 p_log_message := 'Start f_calculate_plan_mart',
									 p_location := 'Sales plan calculation');
									
	v_table_name = 'std3_47.plan_fact_'||p_month;
	v_load_interval = '1 month'::INTERVAL;
	v_start_date := date_trunc('month', to_date(p_month, 'YYYYMM'));
	v_end_date := date_trunc('month', to_date(p_month, 'YYYYMM')) + v_load_interval;
	EXECUTE 'drop view if exists std3_47.plan_fact';
	EXECUTE 'drop table if exists '||v_table_name;


	v_sql = 'create table '||v_table_name||' (region varchar(20), matdirec varchar(20), distr_chan varchar(100), planed_quantity int4,
											  real_quantity int4, percentage_completed int4, material varchar(20))
	with (
		appendonly=true,
		orientation=column,
		compresstype=zstd,
		compresslevel=1
	)
	distributed by (distr_chan);';

	RAISE NOTICE 'TABLE IS: %', v_sql;
	EXECUTE v_sql;
	v_sql = ' with total_product as (select s.material, s.region, sum(s.quantity) as total
		from std3_47.sales s 
		where s."date" between '''||v_start_date||''' and '''||v_end_date||''' 
		group by s.region, s.material) , 
	total_region as (
		select p.region, p.matdirec, p.distr_chan, sum(p.quantity) as planned_quantity, sum(s.quantity) as real_quantity, 
			((sum(s.quantity) * 100)/sum(p.quantity)) as percentage_completed
		from std3_47.sales s join std3_47.plan p on (s.region = p.region and s."date" = p."date" and s.distr_chan = p.distr_chan) 
			join total_product tp on (tp.material = s.material and tp.region = s.region)
		where p."date" between '''||v_start_date||''' and '''||v_end_date||'''
		group by p.region, p.matdirec, p.distr_chan),
	max_qnt as (select tp.region, max(tp.total) as max_quantity
		from total_product tp 
		group by tp.region)
	insert into '||v_table_name||' (region, matdirec, distr_chan, planed_quantity, real_quantity, percentage_completed, material)
		select tr.region, tr.matdirec, tr.distr_chan, planned_quantity, real_quantity, percentage_completed, tp.material
		from total_region tr join total_product tp on tp.region = tr.region join max_qnt mq on mq.region = tr.region 
		where mq.max_quantity = tp.total;';
	RAISE NOTICE 'TABLE IS: %', v_sql;

	EXECUTE v_sql;

	v_sql = 'CREATE VIEW std3_47.v_plan_fact AS
	SELECT p.region, p.matdirec, p.distr_chan, p.planed_quantity, p.real_quantity, p.percentage_completed, p.material, pr.brand, pr.txt, pc.price
	from '||v_table_name||' p join std3_47.product pr on p.material = pr.material join std3_47.price pc on pc.material = p.material;
	';
	EXECUTE v_sql;

	EXECUTE 'SELECT COUNT(1) FROM '||v_table_name INTO v_return;


	PERFORM std3_76.f_load_write_log(p_log_type := 'INFO',
									 p_log_message := v_return ||' rows inserted',
									 p_location := 'Sales plan calculation');
	PERFORM std3_76.f_load_write_log(p_log_type := 'INFO',
									 p_log_message := 'End f_calculate_plan_mart',
									 p_location := 'Sales plan calculation');
	RETURN v_return;
END;
$$
EXECUTE ON ANY;
--

DROP FUNCTION std3_47.f_load_full(TEXT, TEXT);
DROP FUNCTION std3_47.f_load_simple_partition(TEXT, TEXT, timestamp, timestamp, TEXT, TEXT, TEXT);
DROP FUNCTION std3_47.f_calculate_plan_mart(varchar);

-- Full load to reference table
SELECT std3_47.f_load_full('std3_47.price', 'price');
SELECT std3_47.f_load_full('std3_47.chanel', 'chanel');
SELECT std3_47.f_load_full('std3_47.product', 'product');
SELECT std3_47.f_load_full('std3_47.region', 'region');

-- Delta partition load to fact table
SELECT std3_47.f_load_simple_partition('std3_47.sales', '"date"', '2021-01-02', '2021-07-27', 'gp.sales', 'intern', 'intern');
SELECT * FROM std3_47.sales;

-- Create logs table
CREATE TABLE std3_47.logs (
	log_id int8 NOT NULL,
	log_timestamp timestamp NOT NULL DEFAULT now(),
	log_type TEXT NOT NULL,
	log_msg TEXT NOT NULL,
	log_location TEXT NULL,
	is_error bool NULL,
	log_user TEXT NULL DEFAULT "current_user"(),
	CONSTRAINT pk_log_id PRIMARY KEY (log_id)
)
DISTRIBUTED BY (log_id);

-- Create log sequence
CREATE SEQUENCE std3_47.log_id_seq
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 2147483647
	START 1;


