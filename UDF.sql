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

DROP FUNCTION std3_47.f_load_full(TEXT, TEXT);

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

DROP FUNCTION std3_47.f_load_full(TEXT, TEXT);
DROP FUNCTION std3_47.f_load_simple_partition(TEXT, TEXT, timestamp, timestamp, TEXT, TEXT, TEXT);

-- Full load to reference table
SELECT std3_47.f_load_full('std3_47.price', 'price');
SELECT std3_47.f_load_full('std3_47.chanel', 'chanel');
SELECT std3_47.f_load_full('std3_47.product', 'product');
SELECT std3_47.f_load_full('std3_47.region', 'region');

-- Delta partition load to fact table
SELECT std3_47.f_load_simple_partition('std3_47.sales', '"date"', '2021-01-02', '2021-07-27', 'gp.sales', 'intern', 'intern');
SELECT * FROM std3_47.sales;




