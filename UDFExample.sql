-- Auxiliary funcs

-- Unify
CREATE OR REPLACE FUNCTION std3_47.f_unify_name(p_name TEXT)
	RETURNS int8
	LANGUAGE plpgsql
	VOLATILE
AS $$
DECLARE
BEGIN
	RETURN lower(trim(TRANSLATE(p_name, ';/''', '')));
END;
$$
EXECUTE ON ANY;

-- Truncate
CREATE OR REPLACE FUNCTION std3_47.f_truncate_table(p_table_name TEXT)
	RETURNS int8
	LANGUAGE plpgsql
	VOLATILE
AS $$
DECLARE
	v_table_name TEXT;
	v_sql TEXT;
BEGIN
	v_table_name := std3_47.f_unify_name(p_name := p_table_name);
	v_sql := 'TRUNCATE TABLE ' ||v_table_name;
	EXECUTE v_sql;
END;
$$
EXECUTE ON ANY;

-- Get delta table name
CREATE OR REPLACE FUNCTION std3_47.f_get_delta_table_name(p_name TEXT)
	RETURNS int8
	LANGUAGE plpgsql
	VOLATILE
AS $$
DECLARE
	v_full_table_name TEXT;
	v_tmp_table_name TEXT;
	v_table_name TEXT;
	v_schema_name TEXT;
BEGIN
	v_full_table_name = std3_47.f_unify_name(p_name := v_full_table_name);
	v_schema_name = LEFT(v_full_table_name, POSITION('.' IN v_full_table_name) - 1);
	v_schema_name = 'stg '||v_schema_name;
	v_table_name = RIGHT(v_full_table_name, length(v_full_table_name) - POSITION('.' IN v_full_table_name));
	v_tmp_table_name = v_schema_name||'.'||'delta '||v_table_name;

	RETURN v_tmp_table_name;
END;
$$
EXECUTE ON ANY;

-- Get external table name

-- Create temp table
CREATE OR REPLACE FUNCTION std3_47.f_create_tmp_table(p_table_name TEXT, p_prefix_name TEXT)
	RETURNS int8
	LANGUAGE plpgsql
	VOLATILE
AS $$
DECLARE
	v_table_name TEXT;
	v_full_table_name TEXT;
	v_tmp_t_name TEXT;
	v_storage_param TEXT;
	v_sql TEXT;
	v_schema_name TEXT;
	v_dist_key TEXT;
BEGIN
	v_table_name := std3_47.f_unify_name(p_name := p_table_name);
	v_full_table_name := std3_47.f_unify_name(p_name := v_table_name);

	v_schema_name = LEFT(v_full_table_name, POSITION('.' IN v_full_table_name) - 1);
	v_table_name = RIGHT(v_full_table_name, length(v_full_table_name) - POSITION('.' IN v_full_table_name));
	v_tmp_t_name = v_schema_name||'.'||p_prefix_name||v_table_name;
	v_tmp_t_name = std3_47.f_unify_name(p_name := v_tmp_t_name);
	v_storage_param = std3_47.f_get_table_attributes(p_table_name := v_full_table_name);
	v_dist_key = std3_47.f_get_distribution_key(p_table_name := v_full_table_name);
	
	v_sql := 'CREATE TEMP TABLE ' || v_tmp_t_name || ' (like ' || v_full_table_name || ') ' || v_storage_param||' '||v_dist_key||';';
	EXECUTE v_sql;
	RETURN v_tmp_t_name;
END;
$$
EXECUTE ON ANY;

-- Merge table to and table from (FOR COPY)
INSERT INTO v_buffer_table_name
	SELECT v_table_cols
	FROM (
		SELECT q.*, RANK() OVER (PARTITION BY v_merge_key ORDER BY rnk) AS rnk_f
		FROM (
			SELECT v_table_cols, '1' rnk
			FROM v_table_from_name f
			UNION ALL
			SELECT v_table_cols, '2' rnk
			FROM v_table_to_name t
			) q
		) qr
	WHERE rnk_f = 1
	
-- Switch default partition
CREATE OR REPLACE FUNCTION std3_47.switch_def_partition(p_table_from_name TEXT, p_table_to_name TEXT)
	RETURNS void
	LANGUAGE plpgsql
	VOLATILE
AS $$
DECLARE
BEGIN
	EXECUTE 'ALTER TABLE '||v_table_to_name||' EXCHANGE DEFAULT PARTITION WITH TABLE '||v_table_from_name||' ;';
END;
$$
EXECUTE ON ANY;

-- Switch partition
CREATE OR REPLACE FUNCTION std3_47.switch_partition(p_table_name TEXT, p_partition_value timestamp, p_switch_table_name TEXT)
	RETURNS void
	LANGUAGE plpgsql
	VOLATILE
AS $$
DECLARE
	v_rank TEXT; -- ?
BEGIN
	EXECUTE 'ALTER TABLE '||v_table_name||' EXCHANGE PARTITION FOR (RANK('||TO_CHAR(v_rank, '999999999')||')) WITH TABLE '||v_switch_table_name||' WITH VALIDATION;';
END;
$$
EXECUTE ON ANY;

-- Create date partitions
CREATE OR REPLACE FUNCTION std3_47.f_create_date_partition(p_table_name TEXT, p_partition_value timestamp)
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
	v_ts_format TEXT := 'YYYY-MM-DD HH24:MI:SS'
BEGIN
	v_table_name = std3_47.unify_name(p_table_name);
	-- Check having table partition
	SELECT count(*) INTO v_cnt_partitions FROM pg_partition p WHERE p.schemaname||'.'||p.tablename = lower(v_table_name);
	IF v_cnt_partitions > 1 THEN
		LOOP
			SELECT partitionrangeend INTO v_partition_end_sql
				FROM (
					SELECT p.*, RANK() OVER (ORDER BY partitionrank DESC) rnk FROM pg_partition p
					WHERE p.partitionrank IS NOT NULL AND p.schemaname||'.'||p.tablename = lower(v_table_name)
					) q
				WHERE rnk = 1;
			-- Last partition end date
			EXECUTE 'SELECT '||v_partition_end_sql INTO v_partition_end;
			-- If partition already exists for input value, then exit from function
			EXIT WHEN v_partition_end > p_partition_value;
			v_interval := '1 month'::INTERVAL;
			-- Cut new partition from default partition, if it is not exist
			EXECUTE 'ALTER TABLE '||v_table_name||' SPLIT DEFAULT PARTITION
					START ('||v_partition_end_sql||') END ('''||to_char(v_partition_end+v_interval, v_ts_format)||'''::timestamp)'; 
		END LOOP;
	END IF;
END;
$$
EXECUTE ON ANY;

-- CHANGING GUIDE TABLE

-- ELT FULL
CREATE OR REPLACE FUNCTION std3_47.f_full_load(p_table_from TEXT, p_table_to TEXT, p_where TEXT, p_truncate_tgt bool)
	RETURNS int8
	LANGUAGE plpgsql
	SECURITY DEFINER
	VOLATILE
AS $$

DECLARE
	v_table_from TEXT;
	v_table_to TEXT;
	v_where TEXT;
	v_cnt int8;
BEGIN
	v_table_from = std3_47.f_unify_name(p_name := p_table_from);
	v_table_to = std3_47.f_unify_name(p_name := p_table_to);
	v_where = COALESCE(p_where, '1=1');

	IF COALESCE(p_truncate_tgt, FALSE) IS TRUE THEN
		PERFORM std3_47.f_truncate_table(v_table_to);
	END IF;

	EXECUTE 'INSERT INTO '||v_table_to||' SELECT * FROM '||v_table_from||' WHERE '||v_where;
	GET DIAGNOSTICS v_cnt = ROW_COUNT;
	RAISE NOTICE '% rows inserted from % into %', v_cnt, v_table_from, v_table_to;
	RETURN v_cnt;
END;
$$
EXECUTE ON ANY;

-- ELT DELTA MERGE
CREATE OR REPLACE FUNCTION std3_47.f_delta_merge_load(p_table_from TEXT, p_table_to TEXT, p_where TEXT, p_merge_key TEXT)
	RETURNS int8
	LANGUAGE plpgsql
	SECURITY DEFINER
	VOLATILE
AS $$

DECLARE
	v_table_from_name TEXT;
	v_table_to_name TEXT;
	v_buffer_table_name TEXT;
	v_table_cols TEXT;
	v_merge_key TEXT;
	v_merge_sql TEXT;
	v_cnt int8;
BEGIN
	v_table_full_name = std3_47.f_unify_name(p_name := p_table_to);
	v_tmp_table_name = std3_47.f_get_delta_table_name(p_name := p_table_to);
	v_ext_table_name = std3_47.f_get_ext_table_name(p_name := p_table_to);

	PERFORM std3_47.f_truncate_table(p_table_name := v_tmp_table_name);
	PERFORM std3_47.f_insert_table(p_table_from := v_ext_table_name, p_table_to := v_tmp_table_name, p_where := p_where);

	v_table_from_name = std3_47.f_unify_name(p_name := v_tmp_table_name);
	v_table_to_name = std3_47.f_unify_name(p_name := v_full_table_name);

	v_buffer_table_name = std3_47.f_create_tmp_table(p_table_name := v_table_to_name, p_prefix_name := 'buffer_');

	SELECT string_agg(column_name, ',' ORDER BY ordinal_position) INTO v_table_cols FROM information_schema.columns
	WHERE table_schema||'.'||table_name = v_table_to_name;

	v_merge_sql := 'check a little bit higher faggot';
	
	EXECUTE v_merge_sql;
	PERFORM std3_47.f_switch_def_partition(p_table_from_name := v_buffer_table_name, p_table_to_name := v_table_to_name);
END;
$$
EXECUTE ON ANY;

-- CHANGING FACT TABLE

-- ELT DELTA PARTITION
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















