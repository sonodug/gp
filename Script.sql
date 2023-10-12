--SELECT std3_47.f_load_write_log('INFO', 'End f_calculate_plan_mart', 'Sales plan calculation');


-- SELECT std3_47.f_calculate_plan_mart('202101');

SELECT * FROM information_schema.columns;

TRUNCATE TABLE std3_47.sales;
TRUNCATE TABLE std3_47.price;
TRUNCATE TABLE std3_47.product;


SELECT std3_47.f_load_simple_partition('std3_47.sales', '"date"', '2021-01-12', '2021-02-01', 'gp.sales', 'intern', 'intern');


SELECT std3_47.f_load_simple_upsert('gp.plan', 'std3_47.plan', '"date"', 'intern', 'intern');

SELECT s."date" FROM std3_47.sales s
JOIN std3_47.plan p ON p.date = s."date"; 


SELECT std3_47.f_load_full('std3_47.price', 'price');
SELECT std3_47.f_load_full('std3_47.region', 'region');