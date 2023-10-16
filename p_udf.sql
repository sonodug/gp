-- Custom
CREATE OR REPLACE FUNCTION std3_47.f_handle()
	RETURNS void
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
						coupon_disc_tab AS (
							SELECT DISTINCT c.receipt_id, c.coupon_id, c.product_id, c.store_id, p.promo_type_id, p.discount_amount,
								CASE 
									WHEN p.promo_type_id = 1 THEN p.discount_amount
									ELSE (((bi.rpa_sat/bi.qty)*p.discount_amount)/100)
								END AS coupon_discount
							FROM std3_47.coupons c
							JOIN std3_47.promos p ON (p.product_id = c.product_id and p.promo_id = c.promo_id)
							JOIN std3_47.bills_item bi ON (bi.billnum = c.receipt_id and c.product_id = bi.material)
							WHERE c."date" >= '''||v_start_date||'''::date AND c."date" < '''||v_iter_date||'''::date),
						sum_disc AS (
							SELECT cd.store_id, sum(cd.coupon_discount) AS sum_disc, count(product_id) AS prod_disc_qnt
							FROM coupon_disc_tab cd
							GROUP BY cd.store_id),
						sum_traffic AS (
							SELECT t.plant, sum(t.quantity) AS t_qnt
							FROM std3_47.traffic t
							WHERE t."date" >= '''||v_start_date||'''::date AND t."date" < '''||v_iter_date||'''::date
							GROUP BY t.plant),
						sum_turnover AS (
							SELECT bh.plant, bh.calday, sum(bi.rpa_sat) AS turnover, sum(bi.qty) AS quant, count(DISTINCT bi.billnum) AS bill_qnt
							FROM std3_47.bills_item bi
							JOIN std3_47.bills_head bh ON bh.billnum = bi.billnum
							WHERE bh.calday >= '''||v_start_date||'''::date AND bh.calday < '''||v_iter_date||'''::date
							GROUP BY bh.plant, bh.calday)
					INSERT INTO '||v_table_name||' (store_id, store_name, turnover, coupon_discount, turnover_with_disc, sold_prod_qnt, bills_qnt, traffic,
												 	prod_coup_qnt, perc_with_disc, avg_prod_qnt, conversion_rate, avg_bill, avg_rev_per_client)
					SELECT s.store_id, s.store_name, turnover, sum_disc, (turnover - sum_disc) AS turnover_with_disc, quant, bill_qnt, t_qnt, prod_disc_qnt,
						   ((prod_disc_qnt*100)/quant) AS perc_with_disc,
						   (quant/bill_qnt) AS avg_prod_qnt,
						   ((CAST(bill_qnt AS numeric(17, 2))*100)/CAST(t_qnt AS numeric(17,2))) AS conversion_rate
						   (turnover/bill_qnt) AS avg_bill,
						   (turnover/t_qnt) AS avg_rev_per_client
					FROM std3_47.stores s
					LEFT JOIN sum_turnover st ON s.store_id = st.plant
					LEFT JOIN sum_disc sd ON s.store_id = sd.store_id
					LEFT JOIN sum_traffic str ON st.plant = str.plant;';
			
			RAISE NOTICE 'INSERT SQL IS: %', v_sql;
			EXECUTE v_sql;
		
			EXECUTE 'SELECT COUNT(1) FROM '||v_table_name INTO v_return;
		
		
			PERFORM std3_47.f_load_write_log(p_log_type := 'INFO',
											 p_log_message := v_return ||' rows inserted',
											 p_location := 'Sales plan calculation');
		
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

-- Usage

