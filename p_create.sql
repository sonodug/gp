-- stores
CREATE TABLE std3_47.stores (
	code text NOT NULL,
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
	plant text NULL,
	"date" date NULL,
	"time" text NULL,
	frame_id text NULL,
	quantity int4 NULL
)
WITH (
	appendonly=true,
	orientation=column,
	compresstype=zstd,
	compresslevel=1
)
DISTRIBUTED BY (plant, frame_id);

-- bills_head
CREATE TABLE std3_47.bills_head (
	billnum int8 NULL,
	plant text NULL,
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
	store_code text NOT NULL,
	"date" date NOT NULL,
	coupon_id text NOT NULL,
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