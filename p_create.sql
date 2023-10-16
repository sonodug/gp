-- stores
CREATE TABLE std3_47.stores (
	plant text NOT NULL,
	txt text NOT NULL
)
WITH (
	appendonly=true,
	orientation=column,
	compresstype=zstd,
	compresslevel=1
)
DISTRIBUTED REPLICATED;

-- traffic
CREATE TABLE std3_47.traffic (
	plant bpchar(4) NULL,
	"date" date NULL,
	"time" bpchar(6) NULL,
	frame_id bpchar(10) NULL,
	quantity int4 NULL
)
WITH (
	appendonly=true,
	orientation=row,
	compresstype=zstd,
	compresslevel=1
)
DISTRIBUTED RANDOMLY;

-- bills_head
CREATE TABLE std3_47.bills_head (
	billnum int8 NULL,
	plant bpchar(4) NULL,
	calday date NULL
)
WITH (
	appendonly=true,
	orientation=column,
	compresstype=zstd,
	compresslevel=1
)
DISTRIBUTED BY (billnum);

-- bills_item
CREATE TABLE std3_47.bills_item (
	billnum int8 NULL,
	billitem int8 NULL,
	material int8 NULL,
	netval numeric(17, 2) NULL,
	tax numeric(17, 2) NULL,
	qty int8 NULL,
	rpa_sat numeric(17, 2) NULL
)
WITH (
	appendonly=true,
	orientation=column,
	compresstype=zstd,
	compresslevel=1
)
DISTRIBUTED BY (billnum, billitem);

-- coupons
CREATE TABLE std3_47.coupons (
	plant text NOT NULL,
	"date" date NOT NULL,
	coupon_num text NOT NULL,
	promo_id text NOT NULL,
	material int8 NOT NULL,
	billnum int8 NOT NULL
)
WITH (
	appendonly=true,
	orientation=column,
	compresstype=zstd,
	compresslevel=1
)
DISTRIBUTED REPLICATED;

-- promos
CREATE TABLE std3_47.promos (
	promo_id text NOT NULL,
	promo_name text NOT NULL,
	promo_type text NOT NULL,
	material int8 NOT NULL,
	discount_amount int8 NOT NULL
)
WITH (
	appendonly=true,
	orientation=column,
	compresstype=zstd,
	compresslevel=1
)
DISTRIBUTED REPLICATED;

-- promo_types
CREATE TABLE std3_47.promo_types (
	type_id int4 NOT NULL,
	txt text NOT NULL
)
WITH (
	appendonly=true,
	orientation=column,
	compresstype=zstd,
	compresslevel=1
)
DISTRIBUTED REPLICATED;

-- Test distribution
SELECT gp_segment_id, count(*)
FROM std3_47.traffic
GROUP BY 1
ORDER BY gp_segment_id;

SELECT gp_segment_id, count(*)
FROM std3_47.bills_head
GROUP BY 1
ORDER BY gp_segment_id;

SELECT gp_segment_id, count(*)
FROM std3_47.bills_item
GROUP BY 1
ORDER BY gp_segment_id;

-- Optional drop
DROP TABLE IF EXISTS std3_47.stores;
DROP TABLE IF EXISTS std3_47.traffic;
DROP TABLE IF EXISTS std3_47.bills_head;
DROP TABLE IF EXISTS std3_47.bills_item;
DROP TABLE IF EXISTS std3_47.coupons;
DROP TABLE IF EXISTS std3_47.promos;
DROP TABLE IF EXISTS std3_47.promo_types;

-- Create models
CREATE TABLE std3_47.stores_model (
	plant text NOT NULL,
	txt text NOT NULL
)
WITH (
	appendonly=true,
	orientation=column,
	compresstype=zstd,
	compresslevel=1
)
DISTRIBUTED REPLICATED;

-- traffic
CREATE TABLE std3_47.traffic_model (
	plant bpchar(4) NULL,
	"date" date NULL,
	"time" bpchar(6) NULL,
	frame_id bpchar(10) NULL,
	quantity int4 NULL
)
WITH (
	appendonly=true,
	orientation=row,
	compresstype=zstd,
	compresslevel=1
)
DISTRIBUTED RANDOMLY;

-- bills_head
CREATE TABLE std3_47.bills_head_model (
	billnum int8 NULL,
	plant bpchar(4) NULL,
	calday date NULL
)
WITH (
	appendonly=true,
	orientation=column,
	compresstype=zstd,
	compresslevel=1
)
DISTRIBUTED BY (billnum);

-- bills_item
CREATE TABLE std3_47.bills_item_model (
	billnum int8 NULL,
	billitem int8 NULL,
	material int8 NULL,
	netval numeric(17, 2) NULL,
	tax numeric(17, 2) NULL,
	qty int8 NULL,
	rpa_sat numeric(17, 2) NULL
)
WITH (
	appendonly=true,
	orientation=column,
	compresstype=zstd,
	compresslevel=1
)
DISTRIBUTED BY (billnum, billitem);

-- coupons
CREATE TABLE std3_47.coupons_model (
	plant text NOT NULL,
	"date" date NOT NULL,
	coupon_num text NOT NULL,
	promo_id text NOT NULL,
	material int8 NOT NULL,
	billnum int8 NOT NULL
)
WITH (
	appendonly=true,
	orientation=column,
	compresstype=zstd,
	compresslevel=1
)
DISTRIBUTED REPLICATED;

-- promos
CREATE TABLE std3_47.promos_model (
	promo_id text NOT NULL,
	promo_name text NOT NULL,
	promo_type text NOT NULL,
	material int8 NOT NULL,
	discount_amount int8 NOT NULL
)
WITH (
	appendonly=true,
	orientation=column,
	compresstype=zstd,
	compresslevel=1
)
DISTRIBUTED REPLICATED;

-- promo_types
CREATE TABLE std3_47.promo_types_model (
	type_id int4 NOT NULL,
	txt text NOT NULL
)
WITH (
	appendonly=true,
	orientation=column,
	compresstype=zstd,
	compresslevel=1
)
DISTRIBUTED REPLICATED;

-- report_model
CREATE TABLE std3_47.report_model (
	plant text NULL,
	txt text NULL,
	turnover int8 NULL,
	coupon_discount numeric(17, 2) NULL,
	turnover_with_discount numeric(17, 2) NULL,
	material_qty int8 NULL,
	bills_qty int8 NULL,
	traffic int8 NULL,
	matdisc_qty int8 NULL,
	matdisc_percent numeric(17, 1) NULL,
	avg_mat_qty numeric(17, 2) NULL,
	conversion_rate numeric(17, 2) NULL,
	avg_bill numeric(17, 1) NULL,
	avg_profit numeric(17, 1) NULL
)
WITH (
	appendonly=true,
	orientation=column,
	compresstype=zstd,
	compresslevel=1
)
DISTRIBUTED REPLICATED;

SELECT std3_47.f_ensure_templating(ARRAY['std3_47.stores_model', 'std3_47.traffic_model', 'std3_47.bills_head_model', 'std3_47.bills_item_model',
										'std3_47.coupons_model', 'std3_47.promos_model', 'std3_47.promo_types_model']);