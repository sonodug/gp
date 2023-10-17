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
CREATE OR REPLACE FUNCTION std3_47.f_p_load_delta_partition(p_table TEXT, p_partition_key TEXT, p_start_date timestamp, p_end_date timestamp, p_conversion boolean DEFAULT FALSE,
	p_ext_tool TEXT DEFAULT 'pxf', p_ext_table TEXT DEFAULT NULL, p_gpf_filename TEXT DEFAULT NULL, p_pxf_user TEXT DEFAULT 'intern', p_pxf_pass TEXT DEFAULT 'intern'::text)
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
	v_ext_table = std3_47.f_unify_name(p_table)||'_ext';
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
			v_sql = 'CREATE EXTERNAL TABLE '||p_ext_table||'(LIKE '||p_table||'_model)
					LOCATION ('''||v_gpfdist||'''
					) ON ALL
					FORMAT ''CSV'' ( HEADER DELIMITER '';'' NULL '''' ESCAPE ''"'' QUOTE ''"'' )
					ENCODING ''UTF8''';			
		WHEN 'pxf' THEN
			v_pxf = 'pxf://'||p_pxf_table||'?PROFILE=Jdbc&JDBC_DRIVER=org.postgresql.Driver&DB_URL=jdbc:postgresql://192.168.214.212:5432/postgres&USER='||p_pxf_user||'&PASS='||p_pxf_pass;
			
			RAISE NOTICE 'PXF CONNECTION STRING: %', v_pxf;
		
			v_sql = 'CREATE EXTERNAL TABLE '||v_ext_table||'(LIKE '||p_table||'_model)
					 LOCATION ('''||v_pxf||'''
					 ) ON ALL
					 FORMAT ''CUSTOM'' (FORMATTER=''pxfwritable_import'')
					 ENCODING ''UTF8''';
			
			EXECUTE v_sql;
		ELSE
			PERFORM std3_47.f_load_write_log(p_log_type := 'ERROR',
						 		 			 p_log_message := 'p_ext_tool: % not exists exception', p_ext_tool,
						 					 p_location := 'Report');
			RAISE EXCEPTION 'Provided external tool not exists';				
	END CASE;
			
	--RAISE NOTICE 'EXTERNAL TABLE IS: %', v_sql;
	
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
		RAISE NOTICE 'INSERTED ROWS: %', v_cnt;
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
CREATE OR REPLACE FUNCTION std3_47.f_p_create_date_partitions(p_table_name TEXT, p_partition_value timestamp)
	RETURNS int8
	LANGUAGE plpgsql
	VOLATILE
AS $$
DECLARE
	v_cnt_partitions int;
	v_table_name TEXT;
	v_partition_end_sql TEXT;
	v_partition_end timestamp;
	v_interval INTERVAL;
	v_ts_format TEXT := 'YYYY-MM-DD';
	v_cnt int4;
BEGIN
	v_table_name = std3_47.f_unify_name(p_name := p_table_name);

	SELECT count(*) INTO v_cnt_partitions FROM pg_partition p WHERE p.schemaname||'.'||p.tablename = lower(v_table_name);
	v_cnt := 0;
	IF v_cnt_partitions > 1 THEN
		LOOP
			SELECT partitionrangeend INTO v_partition_end_sql
				FROM (
					SELECT p.*, RANK() OVER (ORDER BY partitionrank DESC) rnk FROM pg_partition p
					WHERE p.partitionrank IS NOT NULL AND p.schemaname||'.'||p.tablename = lower(v_table_name)
					) q
				WHERE rnk = 1;

			EXECUTE 'SELECT '||v_partition_end_sql INTO v_partition_end;
			EXIT WHEN v_partition_end > p_partition_value;
			v_interval := '1 month'::INTERVAL;
			v_cnt := v_cnt + 1;
			EXECUTE 'ALTER TABLE '||v_table_name||' SPLIT DEFAULT PARTITION
					START ('||v_partition_end_sql||') END ('''||to_char(v_partition_end+v_interval, v_ts_format)||'''::timestamp)'; 
		END LOOP;
	END IF;
	RETURN v_cnt;
END;
$$
EXECUTE ON ANY;

--
CREATE OR REPLACE FUNCTION std3_47.f_build_report_mart(p_start_date TEXT, p_load_interval int4 DEFAULT 2, p_end_date TEXT DEFAULT NULL, p_selection_modes selection_mode[])
	RETURNS void
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
	v_return TEXT;
BEGIN
	PERFORM std3_47.f_load_write_log(p_log_type := 'INFO',
									 p_log_message := 'Start f_build_report_mart',
									 p_location := 'Report');
	
	v_valid_modes := enum_range(NULL::selection_mode);

	-- Initial verification
	IF array_length(p_selection_mode, 1) IS NULL THEN
		PERFORM std3_47.f_load_write_log(p_log_type := 'ERROR',
								 		 p_log_message := 'Parameters for p_selection_modes not provided',
								 		 p_location := 'Report');
		RAISE EXCEPTION 'Parameters for p_selection_modes not provided';
	END IF;
	
	FOR it_mode IN SELECT UNNEST(p_selection_modes) LOOP
		IF NOT it_mode = ANY(v_valid_modes) THEN
			PERFORM std3_47.f_load_write_log(p_log_type := 'ERROR',
						 		 			 p_log_message := 'Invalid value detected in p_selection_modes',
						 					 p_location := 'Report');
			RAISE EXCEPTION 'Invalid value detected in p_selection_modes';
		END IF;
	END LOOP;
	
	BEGIN
		PERFORM to_date(v_start_date, 'YYYYMMDD');
		PERFORM to_date(p_end_date, 'YYYYMMDD');
		EXCEPTION
			WHEN OTHERS THEN
				PERFORM std3_47.f_load_write_log(p_log_type := 'ERROR',
							 		 			 p_log_message := 'Invalid Format',
							 					 p_location := 'Report');				
				RAISE EXCEPTION "Date % - % doesn't matching the required format. Check your input", v_start_date, v_end_date;
	END;

	-- Handle
	FOR it_mode IN SELECT UNNEST(p_selection_modes) LOOP
		CASE it_mode
			WHEN 'daily' THEN
				v_load_interval = INTERVAL '1 day';
				v_current_format = 'YYYYMMDD';
	            v_start_date := to_date(p_start_date, 'YYYYMMDD');
	            v_end_date := to_date(p_end_date, 'YYYYMMDD');
	           	v_table_name = 'std3_47.report_'||to_char(v_start_date, v_current_format);
	            v_intervals_num = floor(EXTRACT(epoch FROM age(v_end_date, v_start_date)) / EXTRACT(epoch FROM v_load_interval));
			WHEN 'day_inverval' THEN
				v_load_interval = p_load_interval * INTERVAL '1 day';
				v_current_format = 'YYYYMMDD';
	            v_start_date := to_date(p_start_date, 'YYYYMMDD');
	            v_end_date := v_start_date + v_load_interval;
				v_table_name = 'std3_47.report_'||p_start_date||'_'||v_end_date;
				v_intervals_num = 1;
			WHEN 'monthly' THEN
				v_load_interval = INTERVAL '1 month';
				v_current_format = 'YYYYMM';
	            v_start_date := to_date(p_start_date, 'YYYYMMDD');
	            v_end_date := to_date(p_end_date, 'YYYYMMDD');
	           	v_table_name = 'std3_47.report_'||to_char(v_start_date, v_current_format);
	           	v_intervals_num = floor(EXTRACT(epoch FROM age(v_end_date, v_start_date)) / EXTRACT(epoch FROM v_load_interval));
			WHEN 'month_interval' THEN
				v_load_interval = p_load_interval * INTERVAL '1 month';
				v_current_format = 'YYYYMM';
	            v_start_date := to_date(p_start_date, 'YYYYMMDD');
	            v_end_date := v_start_date + v_load_interval;
				v_table_name = 'std3_47.report_'||to_char(v_start_date, v_current_format)||'_'||to_char(v_start_date, v_current_format);
				v_intervals_num = 1;
			ELSE
				PERFORM std3_47.f_load_write_log(p_log_type := 'ERROR',
							 		 			 p_log_message := 'Invalid value detected in p_selection_modes',
							 					 p_location := 'Report');
				RAISE EXCEPTION 'Invalid value detected in p_selection_modes';
		END CASE;
	
		EXECUTE 'DROP TABLE IF EXISTS '||v_table_name;  
		
	    FOR i IN 1..v_num_intervals LOOP
			v_iter_date := v_start_date + i * v_load_interval;
		
			v_sql = 
			'CREATE TABLE '||v_table_name||'
			 LIKE std3_47.report_model';
			
			RAISE NOTICE 'REPORT TABLE IS: %', v_sql;
			EXECUTE v_sql;
		
			v_sql = 'WITH 
						coupon_discount AS (
							SELECT DISTINCT c.billnum, c.coupon_id, c.material, c.plant, p.promo_type, p.discount_amount,
								CASE 
									WHEN p.promo_type = 1 THEN p.discount_amount
									ELSE (((bi.rpa_sat / bi.qty) * p.discount_amount) / 100)
								END AS coupon_discount
							FROM std3_47.coupons c
							JOIN std3_47.promos p ON (p.material = c.material and p.promo_id = c.promo_id)
							JOIN std3_47.bills_item bi ON (bi.billnum = c.billnum and c.material = bi.material)
							WHERE c."date" >= '''||v_start_date||'''::date AND c."date" < '''||v_iter_date||'''::date),
						sum_discount AS (
							SELECT cd.plant, sum(cd.coupon_discount) AS sum_disc, count(cd.material) AS matdisc_qty
							FROM coupon_discount cd
							GROUP BY cd.plant),
						sum_traffic AS (
							SELECT t.plant, sum(t.quantity) AS t_qty
							FROM std3_47.traffic t
							WHERE t."date" >= '''||v_start_date||'''::date AND t."date" < '''||v_iter_date||'''::date
							GROUP BY t.plant),
						sum_turnover AS (
							SELECT bh.plant, bh.calday, sum(bi.rpa_sat) AS turnover, sum(bi.qty) AS quant, count(DISTINCT bi.billnum) AS bill_qty
							FROM std3_47.bills_item bi
							JOIN std3_47.bills_head bh ON bh.billnum = bi.billnum
							WHERE bh.calday >= '''||v_start_date||'''::date AND bh.calday < '''||v_iter_date||'''::date
							GROUP BY bh.plant, bh.calday)
					INSERT INTO '||v_table_name||' (plant, txt, turnover, coupon_discount, turnover_with_discount, material_qty, bills_qty, traffic,
												 	matdisc_qty, matdisc_percent, avg_mat_qty, conversion_rate, avg_bill, avg_profit)
					SELECT s.plant, s.txt, st.turnover, sum_disc, (st.turnover - sd.sum_disc) AS turnover_with_disc, st.quant, st.bill_qty, str.t_qty, cd.matdisc_qty,
						   ((cd.matdisc_qty * 100) / st.quant) AS matdisc_percent,
						   (st.quant / st.bill_qty) AS avg_mat_qty,
						   ((CAST(st.bill_qty AS numeric(17, 2)) * 100) / CAST(str.t_qty AS numeric(17,2))) AS conversion_rate
						   (st.turnover / st.bill_qty) AS avg_bill,
						   (st.turnover / str.t_qty) AS avg_profit
					FROM std3_47.stores s
					LEFT JOIN sum_turnover st ON s.plant = st.plant
					LEFT JOIN sum_discount sd ON s.plant = sd.plant
					LEFT JOIN sum_traffic str ON st.plant = str.plant;';
			
			RAISE NOTICE 'INSERT SQL IS: %', v_sql;
			EXECUTE v_sql;
		
			EXECUTE 'SELECT COUNT(1) FROM '||v_table_name INTO v_return;
		
		
			PERFORM std3_47.f_load_write_log(p_log_type := 'INFO',
											 p_log_message := v_return ||' rows inserted',
											 p_location := 'Report');
		
			-- if v_num_intervals > 1 we prepared next table
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

	PERFORM std3_47.f_load_write_log(p_log_type := 'INFO',
									 p_log_message := 'End f_build_report_mart',
									 p_location := 'Report');
END;
$$
EXECUTE ON ANY;

-- Drop optional statements
DROP FUNCTION std3_47.f_handle();
DROP FUNCTION std3_47.f_ensure_templating(TEXT[]);
DROP FUNCTION std3_47.f_build_report_mart(TEXT, int4, TEXT, selection_mode[]);
DROP FUNCTION f_p_load_delta_partition(text,text,timestamp without time zone,timestamp without time zone,boolean,text,text,text,text,text);


-- Usage

SELECT std3_47.f_load_full('std3_47.price', 'price');
SELECT std3_47.f_load_simple_partition('std3_47.sales', '"date"', '2021-01-02', '2021-07-27', 'gp.sales', 'intern', 'intern');


-- full: promos, promo_types, stores
SELECT std3_47.f_p_load_full('std3_47.promos', 'promos');
SELECT std3_47.f_p_load_full('std3_47.promo_types', 'promo_types');
SELECT std3_47.f_p_load_full('std3_47.stores', 'stores');
SELECT std3_47.f_p_load_full('std3_47.coupons', 'coupons');

SELECT std3_47.f_p_load_delta_partition('std3_47.traffic', '"date"', '2021-01-01'::timestamp, '2021-02-28'::timestamp,
	p_conversion := TRUE, p_ext_tool := 'pxf', p_ext_table := 'gp.traffic');

SELECT std3_47.f_p_load_delta_partition('std3_47.coupons', '"date"', '2021-01-01'::timestamp, '2021-02-28'::timestamp,
	p_conversion := FALSE, p_ext_tool := 'gpfdist', p_gpf_filename := 'coupons');

SELECT * FROM pg_class;
