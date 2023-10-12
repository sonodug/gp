--SELECT std3_47.f_load_write_log('INFO', 'End f_calculate_plan_mart', 'Sales plan calculation');


-- SELECT std3_47.f_calculate_plan_mart('202101');

SELECT * FROM information_schema.columns;

TRUNCATE TABLE std3_47.sales;
TRUNCATE TABLE std3_47.price;
TRUNCATE TABLE std3_47.product;

SELECT std3_47.f_load_full('std3_47.price', 'price');
SELECT std3_47.f_load_full('std3_47.region', 'region');