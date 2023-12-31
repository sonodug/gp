-- Custom
CREATE OR REPLACE FUNCTION std3_47.f_handle()
	RETURNS TRIGGER
	LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'Cannot modify model';
END;
$$;

CREATE OR REPLACE FUNCTION std3_47.f_ensure_templating(p_tables _text)
	RETURNS void
	LANGUAGE plpgsql
	VOLATILE
AS $$	
DECLARE
	v_table_name TEXT;
BEGIN
    FOR v_table_name IN SELECT unnest(p_tables) LOOP
	    v_table_name := std3_47.f_unify_name(p_name := v_table_name);
        EXECUTE
            'CREATE TRIGGER prevent_insert_trigger
            BEFORE INSERT OR UPDATE OR DELETE ON '||v_table_name||'
            EXECUTE PROCEDURE std3_47.f_handle();';
    END LOOP;
END;
$$
EXECUTE ON ANY;

CREATE OR REPLACE FUNCTION std3_47.f_unify_name(p_name TEXT)
	RETURNS TEXT
	LANGUAGE plpgsql
	VOLATILE
AS $$
DECLARE
BEGIN
	RETURN lower(trim(TRANSLATE(p_name, ';/''', '')));
END;
$$
EXECUTE ON ANY;
--
CREATE OR REPLACE FUNCTION std3_47.f_p_load_full(p_table TEXT, p_file_name TEXT, p_ext_tool TEXT DEFAULT 'gpfdist', p_pxf_user TEXT DEFAULT 'intern', p_pxf_pass TEXT DEFAULT 'intern')
	RETURNS int4
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
DECLARE
	v_ext_table_name TEXT;
	v_sql TEXT;
	v_gpfdist TEXT;
	v_pxf TEXT;
	v_result int4;
BEGIN
	v_ext_table_name = std3_47.f_unify_name(p_table)||'_ext';
	EXECUTE 'TRUNCATE TABLE '||p_table;
	EXECUTE 'DROP EXTERNAL TABLE IF EXISTS '||v_ext_table_name;
	
	CASE p_ext_tool
		WHEN 'gpfdist' THEN
			v_gpfdist = 'gpfdist://172.16.128.34:8080/'||p_file_name||'.csv';
			v_sql = 'CREATE EXTERNAL TABLE '||v_ext_table_name||'(LIKE '||p_table||'_model)
					LOCATION ('''||v_gpfdist||'''
					) ON ALL
					FORMAT ''CSV'' ( HEADER DELIMITER '';'' NULL '''' ESCAPE ''"'' QUOTE ''"'' )
					ENCODING ''UTF8''';			
		WHEN 'pxf' THEN
			v_pxf = 'pxf://gp.'||p_table||'?PROFILE=Jdbc&JDBC_DRIVER=org.postgresql.Driver&DB_URL=jdbc:postgresql://192.168.214.212:5432/postgres&USER='||p_pxf_user||'&PASS='||p_pxf_pass;
			
			RAISE NOTICE 'PXF CONNECTION STRING: %', v_pxf;
		
			v_sql = 'CREATE EXTERNAL TABLE '||v_ext_table||'(LIKE '||p_table||'_model)
					 LOCATION ('''||v_pxf||'''
					 ) ON ALL
					 FORMAT ''CUSTOM'' (FORMATTER=''pxfwritable_import'')
					 ENCODING ''UTF8''';			
		ELSE
			PERFORM std3_47.f_load_write_log(p_log_type := 'ERROR',
						 		 			 p_log_message := 'p_ext_tool: % not exists exception', p_ext_tool,
						 					 p_location := 'Report');
			RAISE EXCEPTION 'Provided external tool not exists';				
	END CASE;
	
	RAISE NOTICE 'EXTERNAL TABLE IS: %', v_sql;
	EXECUTE v_sql;
	EXECUTE 'INSERT INTO '||p_table||' SELECT * FROM '||v_ext_table_name;
	EXECUTE 'SELECT COUNT(1) FROM '||p_table INTO v_result;
	RETURN v_result;
END;

$$
EXECUTE ON ANY;
--
CREATE OR REPLACE FUNCTION std3_47.f_p_load_delta_partition_merge(p_table text, p_partition_key text, p_start_date timestamp, p_end_date timestamp, p_pxf_table_1 text, p_pxf_table_2 text, p_user text, p_pass text)
	RETURNS int4
	LANGUAGE plpgsql
	VOLATILE
AS $$	
DECLARE
	v_ext_table_1 	TEXT;
	v_ext_table_2 	TEXT;
	v_temp_table 	TEXT;
	v_sql 			TEXT;
	v_pxf 			TEXT;
	v_result 		int;
	v_dist_key 		TEXT;
	v_params 		TEXT;
	v_where 		TEXT;
	v_where_join 	TEXT;
	v_load_interval interval;
	v_start_date 	date;
	v_end_date 		date;
	v_table_oid 	int4;
	v_cnt 			int8;
	v_iterDate		timestamp;
	v_date_format 	TEXT := 'DD.MM.YYYY';
	v_params_ext_1	TEXT;
	v_params_ext_2	TEXT;
	v_params_temp	TEXT;
	v_join_on		TEXT;
	v_error			TEXT;
	v_min			TEXT;
	v_max			TEXT;
BEGIN
	PERFORM std3_47.f_p_create_date_partitions(p_table, p_end_date);

	v_join_on = 'billnum';

	v_params_ext_1 = 'billnum int8,
					billitem int8,
					material int8,
					netval numeric(17, 2),
					tax numeric(17, 2),
					qty int8,
					rpa_sat numeric(17, 2)';
				
	v_params_ext_2 = 'billnum int8,
					plant bpchar(4),
					calday date';
	
	v_params_temp = 'ext_1.billnum, 
					billitem,
					material,
					plant,
					calday,
					to_char(calday, ''YYYYMM'') "calmonth",
					rpa_sat,
					qty,
					netval, 
					tax';

	v_ext_table_1 = std3_47.f_unify_name(p_table)||'_pr_ext_1';
	v_ext_table_2 = std3_47.f_unify_name(p_table)||'_pr_ext_2';
	v_temp_table = std3_47.f_unify_name(p_table)||'_tmp';

	v_result = 0;

	SELECT c.oid INTO v_table_oid
	FROM pg_class AS c INNER JOIN pg_namespace as n on c.relnamespace = n.oid
	WHERE n.nspname||'.'||c.relname = p_table
	LIMIT 1;
	
	IF v_table_oid = 0 OR v_table_oid IS NULL THEN
		v_dist_key = 'DISTRIBUTED RANDOMLY';
	ELSE
		v_dist_key = pg_get_table_distributedby(v_table_oid);
	END IF;
	
	SELECT COALESCE('WITH('||array_to_string(reloptions, ', ')||')','')
	FROM pg_class 
	INTO v_params
	WHERE oid = p_table::regclass;

	RAISE NOTICE 'Params: %', v_params;

	v_load_interval = '1 month'::interval;
	v_start_date := date_trunc('month', p_start_date);
	v_end_date := date_trunc('month', p_end_date) + v_load_interval;

	v_pxf = 'pxf://'||p_pxf_table_1||'?PROFILE=JDBC&JDBC_DRIVER=org.postgresql.Driver&DB_URL=jdbc:postgresql://192.168.214.212:5432/postgres&USER='||p_user||'&PASS='||p_pass;
			
	RAISE NOTICE 'PXF connection string: %', v_pxf;
	
	EXECUTE 'DROP EXTERNAL TABLE IF EXISTS '||v_ext_table_1;

	v_sql = 'CREATE EXTERNAL TABLE '||v_ext_table_1||'('||v_params_ext_1||')
			 LOCATION ('''||v_pxf||'''
			 ) ON ALL
			 FORMAT ''CUSTOM'' (FORMATTER=''pxfwritable_import'')
			 ENCODING ''UTF8''';
			
	RAISE NOTICE 'External table is: %', v_sql;

	EXECUTE v_sql;

	RAISE NOTICE 'Ext_tab_1 created successfully';

	v_pxf = 'pxf://'||p_pxf_table_2||'?PROFILE=JDBC&JDBC_DRIVER=org.postgresql.Driver&DB_URL=jdbc:postgresql://192.168.214.212:5432/postgres&USER='||p_user||'&PASS='||p_pass;
	
	EXECUTE 'DROP EXTERNAL TABLE IF EXISTS '||v_ext_table_2;

	v_sql = 'CREATE EXTERNAL TABLE '||v_ext_table_2||'('||v_params_ext_2||')
			 LOCATION ('''||v_pxf||'''
			 ) ON ALL
			 FORMAT ''CUSTOM'' (FORMATTER=''pxfwritable_import'')
			 ENCODING ''UTF8''';
			
	RAISE NOTICE 'External table is: %', v_sql;

	EXECUTE v_sql;

	RAISE NOTICE 'Ext_tab_2 created successfully';

	LOOP
		v_iterDate = v_start_date + v_load_interval;
	
		EXIT WHEN v_iterDate > v_end_date;

		v_where = p_partition_key||' >= '''||v_start_date||'''::date AND '||p_partition_key||' < '''||v_iterDate||'''::date';
	
		EXECUTE 'SELECT MIN('||v_join_on||')::text, MAX('||v_join_on||')::text
									FROM '||v_ext_table_2||'
									WHERE '||v_where INTO v_min, v_max;
																
		v_where_join = v_join_on||' BETWEEN '||v_min||'::int AND '||v_max||'::int';
	
		v_sql := 'DROP TABLE IF EXISTS '||v_temp_table||';
				  CREATE TABLE '||v_temp_table||' (LIKE '||p_table||') '||v_params||' '||v_dist_key||';';
				 
		RAISE NOTICE 'Temp table is: %', v_sql;
	
		EXECUTE v_sql;
	
		RAISE NOTICE 'Temp_tab created successfully';
	
		v_sql = 'INSERT INTO '||v_temp_table||'
				SELECT '||v_params_temp||'
			 	FROM (SELECT * FROM '||v_ext_table_1||' b WHERE '||v_where_join||') ext_1
				JOIN (SELECT * FROM '||v_ext_table_2||' b WHERE '||v_where||') ext_2 
				ON ext_1.'||v_join_on||'= ext_2.'||v_join_on;
		
		RAISE NOTICE 'Temp table insert: %', v_sql;
		
		EXECUTE v_sql;
		
		GET DIAGNOSTICS v_cnt = row_count;
	
		RAISE NOTICE 'INSERTED ROWS: %', v_cnt;
	
		v_sql = 'ALTER TABLE '||p_table||' EXCHANGE PARTITION FOR (DATE '''||v_start_date||''') WITH TABLE '|| v_temp_table ||' WITH VALIDATION';
		
		RAISE NOTICE 'EXCHANGE PARTITION SCRIPT: %', v_sql;
		
		EXECUTE v_sql;
	
		RAISE NOTICE 'ALTER PART OK for % - %',v_start_date, v_iterDate;
		
		v_result = v_result + v_cnt;
		v_start_date = v_iterDate;
	END LOOP;

	v_sql := 'DROP TABLE IF EXISTS '|| v_temp_table ||'';
	EXECUTE v_sql;

	RETURN v_result;
END;
$$
EXECUTE ON ANY;

--
CREATE OR REPLACE FUNCTION std3_47.f_p_load_delta_partition(p_table text, p_partition_key text, p_start_date timestamp, p_end_date timestamp, p_conversion bool DEFAULT false, p_ext_tool text DEFAULT 'pxf'::text, p_ext_table text DEFAULT NULL::text, p_gpf_filename text DEFAULT NULL::text, p_pxf_user text DEFAULT 'intern'::text, p_pxf_pass text DEFAULT 'intern'::text)
	RETURNS int4
	LANGUAGE plpgsql
	VOLATILE
AS $$
DECLARE
	v_ext_table TEXT;
	v_temp_table TEXT;
	v_sql TEXT;
	v_gpfdist TEXT;
	v_pxf TEXT;
	v_result TEXT;
	v_dist_key TEXT;
	v_params TEXT;
	v_where TEXT;
	v_load_interval INTERVAL;
	v_start_date date;
	v_end_date date;
	v_iter_date date;
	v_table_oid int4;
	v_cnt int8;
	v_i_cnt int8;
BEGIN
	v_ext_table = std3_47.f_unify_name(p_table)||'_pr_ext';
	v_temp_table = std3_47.f_unify_name(p_table)||'_tmp';

	PERFORM std3_47.f_p_create_date_partitions(p_table_name := p_table, p_partition_value := p_end_date);

	v_cnt := 0;

	SELECT c.oid
	INTO v_table_oid
	FROM pg_class AS c INNER JOIN pg_namespace AS n ON c.relnamespace = n.oid
	WHERE n.nspname||'.'||c.relname = p_table
	LIMIT 1;

	IF v_table_oid = 0 OR v_table_oid IS NULL THEN
		v_dist_key := 'DISTRIBUTED RANDOMLY';
	ELSE
		v_dist_key := pg_get_table_distributedby(v_table_oid);
	END IF;

	SELECT COALESCE('WITH (' || array_to_string(reloptions, ', ') || ')', '')
	FROM pg_class
	INTO v_params
	WHERE oid = p_table::regclass;

	RAISE NOTICE 'Params: %', v_params;
	EXECUTE 'DROP EXTERNAL TABLE IF EXISTS '||v_ext_table;

	v_load_interval := '1 month'::INTERVAL;
	v_start_date := date_trunc('month', p_start_date);
	v_end_date := date_trunc('month', p_end_date) + v_load_interval;
	
	CASE p_ext_tool
		WHEN 'gpfdist' THEN
			v_gpfdist = 'gpfdist://172.16.128.34:8080/'||p_gpf_filename||'.csv';
			RAISE NOTICE 'GPFDIST CONNECTION STRING: %', v_gpfdist;
			v_sql = 'CREATE EXTERNAL TABLE '||v_ext_table||'(LIKE '||p_table||'_model)
					LOCATION ('''||v_gpfdist||'''
					) ON ALL
					FORMAT ''CSV'' ( HEADER DELIMITER '';'' NULL '''' ESCAPE ''"'' QUOTE ''"'' )
					ENCODING ''UTF8''';
		WHEN 'pxf' THEN
			v_pxf = 'pxf://'||p_ext_table||'?PROFILE=Jdbc&JDBC_DRIVER=org.postgresql.Driver&DB_URL=jdbc:postgresql://192.168.214.212:5432/postgres&USER='||p_pxf_user||'&PASS='||p_pxf_pass;		
			RAISE NOTICE 'PXF CONNECTION STRING: %', v_pxf;	
			v_sql = 'CREATE EXTERNAL TABLE '||v_ext_table||'(LIKE '||p_table||'_model)
					 LOCATION ('''||v_pxf||'''
					 ) ON ALL
					 FORMAT ''CUSTOM'' (FORMATTER=''pxfwritable_import'')
					 ENCODING ''UTF8''';
		ELSE
			PERFORM std3_47.f_load_write_log(p_log_type := 'ERROR',
						 		 			 p_log_message := 'p_ext_tool: % not exists exception', p_ext_tool,
						 					 p_location := 'Report');
			RAISE EXCEPTION 'Provided external tool not exists';				
	END CASE;

	EXECUTE v_sql;
	RAISE NOTICE 'EXTERNAL TABLE IS: %', v_sql;
	
	LOOP
		v_iter_date := v_start_date + v_load_interval;
	
		EXIT WHEN v_iter_date > v_end_date;
		
		-- src to stg
		v_sql := 'DROP TABLE IF EXISTS '|| v_temp_table ||';
				  CREATE TABLE '|| v_temp_table ||' (LIKE '||p_table||') ' ||v_params||' '||v_dist_key||';';	
		RAISE NOTICE 'TEMP TABLE IS: %', v_sql;
		EXECUTE v_sql;
		
		-- 
		IF p_conversion = TRUE THEN
			v_where := 'to_date(' || p_partition_key || ', ''DD.MM.YYYY'')' || ' >= ''' || v_start_date || '''::date AND ' || 'to_date(' || p_partition_key || ', ''DD.MM.YYYY'')' || ' < ''' || v_iter_date || '''::date';
			v_sql = 'INSERT INTO '||v_temp_table||' SELECT plant, to_date("date", ''DD.MM.YYYY''), "time", frame_id, quantity FROM '||v_ext_table||' WHERE '||v_where||';';
		ELSE
			v_where := p_partition_key || ' >= ''' || v_start_date || '''::date AND ' || p_partition_key || ' < ''' || v_iter_date || '''::date';
			v_sql := 'INSERT INTO ' || v_temp_table || ' SELECT * FROM ' || v_ext_table || ' WHERE ' || v_where;			
		END IF;
		
		--
	
		RAISE NOTICE 'INSERT: %', v_sql;
		EXECUTE v_sql;
	
		GET DIAGNOSTICS v_i_cnt := ROW_COUNT;
		v_cnt := v_cnt + v_i_cnt;
		RAISE NOTICE 'INSERTED ROWS: %', v_i_cnt;
		v_sql = 'ALTER TABLE '||p_table||' EXCHANGE PARTITION FOR (DATE '''||v_start_date||''') WITH TABLE '|| v_temp_table ||' WITH VALIDATION';
		
		RAISE NOTICE 'EXCHANGE PARTITION SCRIPT: %', v_sql;
		EXECUTE v_sql;	
		v_start_date := v_iter_date;
	END LOOP;
	
	v_sql := 'DROP TABLE IF EXISTS '|| v_temp_table ||'';
	EXECUTE v_sql;

	RAISE NOTICE 'INSERTED ROWS: %', v_cnt;
	RETURN v_cnt;
END;
$$
EXECUTE ON ANY;
--

--
CREATE OR REPLACE FUNCTION std3_47.f_p_create_date_partitions(p_table_name text, p_partition_value timestamp)
	RETURNS int8
	LANGUAGE plpgsql
	SECURITY DEFINER
	VOLATILE
AS $$	
declare
	v_table_name text;
	v_cnt int4;
	v_cnt_partitions int;
	v_partition_end_sql text;
	v_partition_end timestamp;
	v_interval interval;
begin
	v_table_name := std3_47.f_unify_name(p_name := p_table_name);
	select count(*) into v_cnt_partitions from pg_partitions p where p.schemaname || '.' || p.tablename = v_table_name;
	v_cnt := 0;
	if v_cnt_partitions > 1 then
		loop
			select partitionrangeend into v_partition_end_sql
			from (select p.*, rank() over (order by partitionrank desc) rnk
				  from pg_partitions p
				  where p.partitionrank is not null
				  and p.schemaname || '.' || p.tablename = v_table_name) q
		    where rnk = 1;
		   	raise notice 'v_partition_end_sql: %', v_partition_end_sql;
		    
			execute 'select ' || v_partition_end_sql into v_partition_end;
			
			exit when p_partition_value < v_partition_end;
		    v_interval := '1 month'::interval;
		    v_cnt := v_cnt + 1;
			execute 'alter table ' || v_table_name || ' split default partition
					 start (''' || v_partition_end || '''::timestamp) end (''' || to_char(v_partition_end + v_interval, 'YYYY-MM-DD') || '''::timestamp)';
			
		end loop;
	END IF;
	return v_cnt;
end;
$$
EXECUTE ON ANY;

--
CREATE OR REPLACE FUNCTION std3_47.f_p_build_report_mart(p_start_date text, p_end_date text, p_load_interval int4 DEFAULT 2, p_selection_modes std3_47."_selection_mode" DEFAULT ARRAY['month_interval'::selection_mode])
	RETURNS int4
	LANGUAGE plpgsql
	SECURITY DEFINER
	VOLATILE
AS $$	
DECLARE
	v_table_name TEXT;
	v_load_interval INTERVAL;
	v_intervals_num int4;
	v_start_date date;
	v_end_date date;
	v_iter_date date;
	v_current_format TEXT;
	v_sql TEXT;
	v_valid_modes selection_mode[];
	v_it_mode selection_mode;
	v_return TEXT;
	v_cnt int4;
	v_i_cnt int4;
	
BEGIN
	PERFORM std3_47.f_load_write_log(p_log_type := 'INFO',
									 p_log_message := 'Start f_build_report_mart',
									 p_location := 'Report');
	
	v_valid_modes := enum_range(NULL::selection_mode);
	v_cnt := 0;

	-- Initial verification
	IF array_length(p_selection_modes, 1) IS NULL THEN
		PERFORM std3_47.f_load_write_log(p_log_type := 'ERROR',
								 		 p_log_message := 'Parameters for p_selection_modes not provided',
								 		 p_location := 'Report');
		RAISE EXCEPTION 'Parameters for p_selection_modes not provided';
	END IF;
	
	FOR i IN 1..array_length(p_selection_modes, 1) LOOP
	    v_it_mode := p_selection_modes[i];
		IF NOT v_it_mode = ANY(v_valid_modes) THEN
			PERFORM std3_47.f_load_write_log(p_log_type := 'ERROR',
						 		 			 p_log_message := 'Invalid value detected in p_selection_modes',
						 					 p_location := 'Report');
			RAISE EXCEPTION 'Invalid value detected in p_selection_modes';
		END IF;
	END LOOP;
	
	BEGIN
		PERFORM to_date(p_start_date, 'YYYYMMDD');
		PERFORM to_date(p_end_date, 'YYYYMMDD');
		EXCEPTION
			WHEN OTHERS THEN
				PERFORM std3_47.f_load_write_log(p_log_type := 'ERROR',
							 		 			 p_log_message := 'Invalid Format',
							 					 p_location := 'Report');				
				RAISE EXCEPTION 'Date % - % doesn''t matching the required format. Check your input', v_start_date, v_end_date;
	END;

	-- Handle
	FOR i IN 1..array_length(p_selection_modes, 1) LOOP
	    v_it_mode := p_selection_modes[i];
		CASE v_it_mode
			WHEN 'daily' THEN
				v_load_interval = INTERVAL '1 day';
				v_current_format = 'YYYYMMDD';
	            v_start_date := to_date(p_start_date, 'YYYYMMDD');
	            v_end_date := to_date(p_end_date, 'YYYYMMDD');
	           	v_table_name := 'std3_47.report_'||to_char(v_start_date, v_current_format);
	            v_intervals_num := floor(EXTRACT(epoch FROM age(v_end_date, v_start_date)) / EXTRACT(epoch FROM v_load_interval));
			WHEN 'day_interval' THEN
				v_load_interval = p_load_interval * INTERVAL '1 day';
				v_current_format = 'YYYYMMDD';
	            v_start_date := to_date(p_start_date, 'YYYYMMDD');
	            v_end_date := v_start_date + v_load_interval;
				v_table_name := 'std3_47.report_'||to_char(v_start_date, v_current_format)||'_'||to_char(v_end_date, v_current_format);
				v_intervals_num := 1;
			WHEN 'monthly' THEN
				v_load_interval = INTERVAL '1 month';
				v_current_format = 'YYYYMM';
	            v_start_date := to_date(p_start_date, 'YYYYMMDD');
	            v_end_date := to_date(p_end_date, 'YYYYMMDD');
	           	v_table_name := 'std3_47.report_'||to_char(v_start_date, v_current_format);
	           	v_intervals_num := floor(EXTRACT(epoch FROM age(v_end_date + INTERVAL '1 day', v_start_date)) / EXTRACT(epoch FROM v_load_interval));
			WHEN 'month_interval' THEN
				v_load_interval = p_load_interval * INTERVAL '1 month';
				v_current_format = 'YYYYMM';
	            v_start_date := to_date(p_start_date, 'YYYYMMDD');
	            v_end_date := v_start_date + (v_load_interval + INTERVAL '1 day');
				v_table_name := 'std3_47.report_'||to_char(v_start_date, v_current_format)||'_'||to_char(v_end_date, v_current_format);
				v_intervals_num := 1;
			ELSE
				PERFORM std3_47.f_load_write_log(p_log_type := 'ERROR',
							 		 			 p_log_message := 'Invalid value detected in p_selection_modes',
							 					 p_location := 'Report');
				RAISE EXCEPTION 'Invalid value detected in p_selection_modes';
		END CASE;
		
	    FOR i IN 1..v_intervals_num LOOP
			v_iter_date := v_start_date + i * v_load_interval;
		
			EXECUTE 'DROP TABLE IF EXISTS '||v_table_name;  
			
			v_sql = 'CREATE TABLE '||v_table_name||' (plant text, txt text, turnover int8, coupon_discount numeric(17, 2), turnover_with_discount numeric(17, 2),
					 material_qty int8, bills_qty int8, traffic int8, matdisc_qty int8, matdisc_percent numeric(17, 1),
					 avg_mat_qty numeric(17, 2), conversion_rate numeric(17, 2), avg_bill numeric(17, 1), avg_profit numeric(17, 2))
					 WITH (
						 appendonly=true,
						 orientation=column,
						 compresstype=zstd,
						 compresslevel=1
					 )
					 DISTRIBUTED REPLICATED;';
			
			RAISE NOTICE 'REPORT TABLE IS: %', v_sql;
			EXECUTE v_sql;
		
			v_sql = 'WITH 
						coupon_disc AS (
							SELECT DISTINCT c.billnum, c.coupon_num, c.material, c.plant, p.promo_type, p.discount_amount,
								CASE 
									WHEN p.promo_type = ''001'' THEN p.discount_amount
									ELSE (((bi.rpa_sat / bi.qty) * p.discount_amount) / 100)
								END AS coupon_discount
							FROM std3_47.coupons c
							JOIN std3_47.promos p ON (p.material = c.material and p.promo_id = c.promo_id)
							JOIN std3_47.bills bi ON (bi.billnum = c.billnum and c.material = bi.material)
							WHERE c."date" >= '''||v_start_date||'''::date AND c."date" < '''||v_iter_date||'''::date),
						sum_discount AS (
							SELECT cd.plant, sum(cd.coupon_discount) AS sum_disc, count(cd.material) AS matdisc_qty
							FROM coupon_disc cd
							GROUP BY cd.plant),
						sum_traffic AS (
							SELECT t.plant, sum(t.quantity) AS t_qty
							FROM std3_47.traffic t
							WHERE t."date" >= '''||v_start_date||'''::date AND t."date" < '''||v_iter_date||'''::date
							GROUP BY t.plant),
						sum_turnover AS (
							SELECT bi.plant, sum(bi.rpa_sat) AS turnover, sum(bi.qty) AS quant, count(DISTINCT bi.billnum) AS bill_qty
							FROM std3_47.bills bi
							WHERE bi.calday >= '''||v_start_date||'''::date AND bi.calday < '''||v_iter_date||'''::date
							GROUP BY bi.plant)
					INSERT INTO '||v_table_name||' (plant, txt, turnover, coupon_discount, turnover_with_discount, material_qty, bills_qty, traffic,
												 	matdisc_qty, matdisc_percent, avg_mat_qty, conversion_rate, avg_bill, avg_profit)
					SELECT s.plant, s.txt, COALESCE(st.turnover, 0), COALESCE(sd.sum_disc, 0), COALESCE((st.turnover - sd.sum_disc), 0) AS turnover_with_disc,
						   COALESCE(st.quant, 0), COALESCE(st.bill_qty, 0), COALESCE(str.t_qty, 0), COALESCE(sd.matdisc_qty, 0),
						   COALESCE(((sd.matdisc_qty * 100) / st.quant), 0) AS matdisc_percent,
						   COALESCE((st.quant / st.bill_qty), 0) AS avg_mat_qty,
						   COALESCE(((CAST(st.bill_qty AS numeric(17, 2)) * 100) / CAST(str.t_qty AS numeric(17,2))), 0) AS conversion_rate,
						   COALESCE((st.turnover / st.bill_qty), 0) AS avg_bill,
						   COALESCE((st.turnover / str.t_qty), 0) AS avg_profit
					FROM std3_47.stores s
					LEFT JOIN sum_turnover st ON s.plant = st.plant
					LEFT JOIN sum_discount sd ON s.plant = sd.plant
					LEFT JOIN sum_traffic str ON st.plant = str.plant;';

			EXECUTE v_sql;
			
			GET DIAGNOSTICS v_i_cnt := ROW_COUNT;
		
			EXECUTE 'SELECT COUNT(1) FROM '||v_table_name INTO v_return;
			RAISE NOTICE 'Number of inserted rows in %: %', v_table_name, v_return;
			v_cnt := v_cnt + v_i_cnt;
			PERFORM std3_47.f_load_write_log(p_log_type := 'INFO',
											 p_log_message := v_return ||' rows inserted',
											 p_location := 'Report');
		
			-- if v_intervals_num > 1 we prepared next table
			CASE v_current_format
				WHEN 'YYYYMMDD' THEN
					v_table_name = 'std3_47.report_'||to_char(v_iter_date, 'YYYYMMDD');
				WHEN 'YYYYMM' THEN
					v_table_name = 'std3_47.report_'||to_char(v_iter_date, 'YYYYMM');
				ELSE
					PERFORM std3_47.f_load_write_log(p_log_type := 'ERROR',
								 		 			 p_log_message := 'v_current_format: % exception', v_current_format,
								 					 p_location := 'Report');
					RAISE EXCEPTION 'Current format handle exception';				
			END CASE;
		END LOOP;
	END LOOP;

	RAISE NOTICE 'Total amount of inserted rows: %', v_cnt;
	PERFORM std3_47.f_load_write_log(p_log_type := 'INFO',
									 p_log_message := 'End f_build_report_mart',
									 p_location := 'Report');
	RETURN v_cnt;
END;



$$
EXECUTE ON ANY;

-- Drop optional statements
DROP FUNCTION std3_47.f_handle();
DROP FUNCTION std3_47.f_ensure_templating(TEXT[]);
DROP FUNCTION std3_47.f_build_report_mart(TEXT, TEXT, int4, selection_mode[]);
DROP FUNCTION f_p_load_delta_partition(text,text,timestamp without time zone,timestamp without time zone,boolean,text,text,text,text,text);


-- Usage

SELECT std3_47.f_load_full('std3_47.price', 'price');
SELECT std3_47.f_load_simple_partition('std3_47.sales', '"date"', '2021-01-02', '2021-07-27', 'gp.sales', 'intern', 'intern');


-- full: promos, promo_types, stores, coupons
SELECT std3_47.f_p_load_full('std3_47.promos', 'promos');
SELECT std3_47.f_p_load_full('std3_47.promo_types', 'promo_types');
SELECT std3_47.f_p_load_full('std3_47.stores', 'stores');
SELECT std3_47.f_p_load_full('std3_47.coupons', 'coupons');

SELECT std3_47.f_p_load_delta_partition('std3_47.traffic', '"date"', '2021-01-01'::timestamp, '2021-02-28'::timestamp,
	p_conversion := TRUE, p_ext_tool := 'pxf', p_ext_table := 'gp.traffic');

SELECT std3_47.f_p_load_delta_partition('std3_47.coupons', '"date"', '2021-01-01'::timestamp, '2021-02-28'::timestamp,
	p_conversion := FALSE, p_ext_tool := 'gpfdist', p_gpf_filename := 'coupons');

SELECT std3_47.f_p_load_delta_partition('std3_47.bills_head', '"calday"', '2020-11-01'::timestamp, '2021-02-28'::timestamp,
	p_conversion := FALSE, p_ext_tool := 'pxf', p_ext_table := 'gp.bills_head');

SELECT std3_47.f_load_simple_upsert('gp.bills_item', 'std3_47.bills_item', '"billnum"', 'intern', 'intern');

SELECT std3_47.f_p_load_delta_partition_merge('std3_47.bills', '"calday"', '2021-01-01', '2021-02-28', 'gp.bills_item', 'gp.bills_head', 'intern', 'intern');

-- Create enum
CREATE TYPE selection_mode AS ENUM ('daily', 'day_interval', 'monthly', 'month_interval');

DROP TYPE selection_mode;

-- check for null, rework debug info
SELECT std3_47.f_p_build_report_mart('20210101', '20210228', 2, ARRAY['month_interval'::selection_mode, 'monthly'::selection_mode]);
SELECT std3_47.f_p_build_report_mart('20210101', '20210105', 4, ARRAY['day_interval'::selection_mode, 'daily'::selection_mode]);

SELECT 'DROP TABLE '||TABLE_NAME||'' 
FROM INFORMATION_SCHEMA.TABLES 
WHERE TABLE_NAME LIKE 'report%';

TRUNCATE TABLE promos;
TRUNCATE TABLE promo_types;
TRUNCATE TABLE stores;
TRUNCATE TABLE traffic;
TRUNCATE TABLE coupons;
TRUNCATE TABLE bills_head;
TRUNCATE TABLE bills_item;
TRUNCATE TABLE bills;

SELECT count(1) FROM traffic;












