SELECT std3_47.f_p_load_delta_partition('std3_47.coupons', '"date"', '2021-01-01'::timestamp, '2021-02-28'::timestamp,
	p_conversion := FALSE, p_ext_tool := 'gpfdist', p_ext_table := 'coupons', p_gpf_filename := 'coupons');



SELECT std3_47.f_p_load_delta_partition('std3_47.traffic', '"date"', '2021-01-01'::timestamp, '2021-02-28'::timestamp, 'gp.traffic');

-- p_table TEXT, p_partition_key TEXT, p_start_date timestamp, p_end_date timestamp,
	p_conversion boolean DEFAULT FALSE,
	p_ext_tool TEXT DEFAULT 'pxf', p_pxf_table TEXT DEFAULT NULL, p_gpf_filename TEXT DEFAULT NULL,
	p_pxf_user TEXT DEFAULT 'intern', p_pxf_pass TEXT DEFAULT 'intern'::text)