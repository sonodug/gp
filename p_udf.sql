CREATE OR REPLACE FUNCTION std3_47.f_load_delta_partitions(p_table_from_name TEXT, p_table_to_name TEXT, p_partition_key TEXT, 
															p_schema_name TEXT, p_start_date timestamp, p_end_date timestamp)
	RETURNS int8
	LANGUAGE plpgsql
	SECURITY DEFINER
	VOLATILE
AS $$

DECLARE
	v_table_from_name TEXT;
	v_table_to_name TEXT;
	v_load_interval INTERVAL;
	v_iterDate timestamp;
	v_where TEXT;
	v_prt_table TEXT;
	v_cnt_prt int8;
	v_cnt int8;
BEGIN
	v_table_from_name = std3_47.f_unify_name(p_name := p_table_from_name);
	v_table_to_name = std3_47.f_unify_name(p_name := p_table_to_name);
	v_cnt = 0;

	PERFORM std3_47.f_create_date_partitions(p_table_name := v_table_to_name, p_partition_value := p_end_date);
	v_load_interval = '1 month'::INTERVAL
	v_start_date := date_trunc('month', p_start_date);
	v_end_date := date_trunc('month', p_end_date) + v_load_interval;

		LOOP
			v_iterDate = v_start_date + v_load_interval;
			EXIT WHEN (v_iterDate > v_end_date);
			v_prt_table = std3_47.f_create_tmp_table(p_table_name := v_table_to_name, p_schema_name := p_schema_name, p_prefix_name := 'prt_',
													p_suffix_name := '_'||to_char(v_start_date, 'YYYYMMDD'))
			v_where = p_partition_key ||'>='''||v_start_date|| '''::timestamp and '||p_partition_key||'<'''||v_iterDate||'''::timestamp';
			v_cnt_prt = std3_47.f_insert_table(p_table_from := v_table_from_name, p_table_to := v_prt_table, p_where := v_where);
			v_cnt = v_cnt + v_cnt_prt;
			PERFORM std3_47.f_switch_partition(p_table_name := v_table_to_name, p_partition_value := v_start_date, p_switch_table_name := v_prt_table);
			EXECUTE 'DROP TABLE '||v_prt_table;
			v_start_date := v_iterDate;
		END LOOP;
	RETURN v_cnt;
END;
$$
EXECUTE ON ANY;

CREATE TYPE selection_mode AS ENUM ('daily', 'day_interval', 'monthly', 'month_interval');
DROP TYPE selection_mode;

-- Custom

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
							SELECT distinct c.receipt_id, c.coupon_id, c.product_id, c.store_id, p.promo_type_id, p.discount_amount,
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

	v_sql = '
		insert into '||v_table_name||' (store_id, store_name, turnover, coupon_discount, turnover_with_disc, sold_prod_qnt, bills_qnt, traffic, 
			prod_coup_qnt, perc_with_disc, avg_prod_qnt, conversion_rate, avg_bill, avg_rev_per_client)
			select s.store_id, s.store_name, turnover, sum_disc, (turnover - sum_disc) as turnover_with_disc, quant, bill_qnt, t_qnt, 
				prod_disc_qnt, ((prod_disc_qnt*100)/quant) as perc_with_disc, 
				(quant/bill_qnt) as avg_prod_qnt, ((CAST(bill_qnt AS numeric(17,2))*100)/CAST(t_qnt AS numeric(17,2))) as conversion_rate, 
				(turnover/bill_qnt) as avg_bill, (turnover/t_qnt) as avg_rev_per_client
			from std3_76.stores s left join sum_turnover st on s.store_id = st.plant left join sum_disc sd on s.store_id = sd.store_id 
				 left join sum_traffic str on st.plant = str.plant;';
	
		execute v_sql;
		
		v_start_date = v_iter_date;
	
	RAISE NOTICE 'INSERT SQL IS: %', v_sql;

	EXECUTE v_sql;

	v_sql = 'CREATE VIEW std3_47.v_plan_fact AS
	SELECT p.region_code, p.matdirec_code, p.distr_chanel, p.planned_quantity, p.real_quantity, p.complete_percent, p.mosteff_material, pr.brand, pr.txt, pc.price
	FROM '||v_table_name||' p
	JOIN std3_47.product pr ON p.mosteff_material = pr.material
	JOIN std3_47.price pc on pc.material = p.mosteff_material;';

	RAISE NOTICE 'VIEW IS: %', v_sql;
	EXECUTE v_sql;

	EXECUTE 'SELECT COUNT(1) FROM '||v_table_name INTO v_return;


	PERFORM std3_47.f_load_write_log(p_log_type := 'INFO',
									 p_log_message := v_return ||' rows inserted',
									 p_location := 'Sales plan calculation');
	PERFORM std3_47.f_load_write_log(p_log_type := 'INFO',
									 p_log_message := 'End f_calculate_plan_mart',
									 p_location := 'Sales plan calculation');
	RETURN v_return;
END;
$$
EXECUTE ON ANY;

CREATE OR REPLACE FUNCTION std3_76.f_sales_report_monthly(p_month varchar)
	RETURNS int4
	LANGUAGE plpgsql
	VOLATILE
AS $$

declare 
	v_table_name text;
	v_sql text;
	v_return text;
	v_start_date date;
	v_end_date date;
	v_load_interval interval;
begin
	perform std3_76.f_load_write_log(p_log_type := 'INFO',
									 p_log_message := 'Start f_sales_report',
									 p_location := 'Sales report calculation');
									
	v_table_name = 'std3_76.sales_report_month_'||p_month;
	v_load_interval = '1 month'::interval;
	v_start_date := date_trunc('month', to_date(p_month, 'YYYYMM'));
	v_end_date := date_trunc('month', to_date(p_month, 'YYYYMM')) + v_load_interval;
	
	execute 'drop table if exists '||v_table_name;


	v_sql = 'create table '||v_table_name||' (
		store_id bpchar(4), store_name varchar(20), turnover int8, coupon_discount numeric(17, 2), turnover_with_disc numeric(17, 2), 
		sold_prod_qnt int8, bills_qnt int8, traffic int8, prod_coup_qnt int8, 
		perc_with_disc numeric(17, 1), avg_prod_qnt numeric(17, 2), conversion_rate numeric(17, 2), avg_bill numeric(17, 1), 
		avg_rev_per_client numeric(17, 1)
	)
	with (
		appendonly=true,
		orientation=column,
		compresstype=zstd,
		compresslevel=1
	)
	distributed replicated;';

	raise notice 'TABLE IS: %', v_sql;
	execute v_sql;
	v_sql = 'with coupon_disc_tab as (select distinct c.receipt_id, c.coupon_id, c.product_id, c.store_id, p.promo_type_id, p.discount_amount,
				case 
					when p.promo_type_id = 1 then p.discount_amount 
					else (((bi.rpa_sat/bi.qty)*p.discount_amount)/100) 
					end as coupon_discount
				FROM std3_76.coupons c join std3_76.promos p on (p.product_id = c.product_id and p.promo_id = c.promo_id) 
				 join std3_76.bills_item bi on (bi.billnum = c.receipt_id and c.product_id  = bi.material)
				where c."date" >= '''||v_start_date||'''::date AND c."date" < '''||v_end_date||'''::date ), 
			 sum_disc as (select cd.store_id, sum(cd.coupon_discount) as sum_disc, count(product_id) as prod_disc_qnt
				from coupon_disc_tab cd 
				group by cd.store_id), 
			sum_traffic as (select t.plant, sum(t.quantity) as t_qnt
				from std3_76.traffic t
				where t."date" >= '''||v_start_date||'''::date AND t."date" < '''||v_end_date||'''::date
				group by t.plant), 
			sum_turnover as (select bh.plant, sum(bi.rpa_sat) as turnover, sum(bi.qty) as quant, count(distinct bi.billnum) as bill_qnt
				from std3_76.bills_item bi join  std3_76.bills_head bh on bh.billnum = bi.billnum
				where bh.calday >= '''||v_start_date||'''::date AND bh.calday < '''||v_end_date||'''::date
				group by bh.plant)
		insert into '||v_table_name||' (store_id, store_name, turnover, coupon_discount, turnover_with_disc, sold_prod_qnt, bills_qnt, traffic, 
			prod_coup_qnt, perc_with_disc, avg_prod_qnt, conversion_rate, avg_bill, avg_rev_per_client)
			select s.store_id, s.store_name, turnover, sum_disc, (turnover - sum_disc) as turnover_with_disc, quant, bill_qnt, t_qnt, 
				prod_disc_qnt, ((prod_disc_qnt*100)/quant) as perc_with_disc, 
				(quant/bill_qnt) as avg_prod_qnt, ((CAST(bill_qnt AS numeric(17,2))*100)/CAST(t_qnt AS numeric(17,2))) as conversion_rate, 
				(turnover/bill_qnt) as avg_bill, (turnover/t_qnt) as avg_rev_per_client
			from std3_76.stores s left join sum_turnover st on s.store_id = st.plant left join sum_disc sd on s.store_id = sd.store_id 
				 left join sum_traffic str on st.plant = str.plant;';
	raise notice 'TABLE IS: %', v_sql;

	execute v_sql;


	execute 'SELECT COUNT(1) FROM '||v_table_name into v_return;

	execute 'analyze '||v_table_name;

	perform std3_76.f_load_write_log(p_log_type := 'INFO',
									 p_log_message := v_return ||' rows inserted',
									 p_location := 'Sales report calculation');
	perform std3_76.f_load_write_log(p_log_type := 'INFO',
									 p_log_message := 'End f_sales_report',
									 p_location := 'Sales report calculation');
	return v_return;
end

$$
EXECUTE ON ANY;

-- Drop optional statements
DROP FUNCTION std3_47.f_calculate_plan_mart(varchar);
