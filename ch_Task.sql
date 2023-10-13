-- Create integration db
CREATE TABLE std3_47.ch_plan_fact_ext
(
	`region_code` String,
	`matdirec_code` String,
	`distr_chanel` UInt32,
	`planned_quantity` UInt32,
	`real_quantity` UInt32,
	`complete_percent` UInt32,
	`mosteff_material` String
)
ENGINE = PostgreSQL('192.168.214.203:5432', 'adb', 'plan_fact_202101', 'std3_47', '5ZnVsbq48zvjW7bQ', 'std3_47');

-- Create dicts
CREATE DICTIONARY std3_47.ch_price_dict
(
	`material` Nullable(String),
	`region` Nullable(String),
	`distr_chan` Nullable(String),
	`price` Nullable(Decimal(8, 2))
)
PRIMARY KEY material
SOURCE(POSTGRESQL(PORT 5432 HOST '192.168.214.203' USER 'std3_47' PASSWORD '5ZnVsbq48zvjW7bQ' DB 'adb' TABLE 'std3_47.price'))
LIFETIME(MIN 200 MAX 500)
LAYOUT(COMPLEX_KEY_HASHED());

CREATE DICTIONARY std3_47.ch_chanel_dict
(
	`distr_chan` UInt32,
	`txtsh` String
)
PRIMARY KEY distr_chan
SOURCE(POSTGRESQL(PORT 5432 HOST '192.168.214.203' USER 'std3_47' PASSWORD '5ZnVsbq48zvjW7bQ' DB 'adb' TABLE 'std3_47.chanel'))
LIFETIME(MIN 200 MAX 500)
LAYOUT(HASHED());

CREATE DICTIONARY std3_47.ch_product_dict
(
	`material` String,
	`asgrp` String,
	`brand` String,
	`matcateg` String,
	`matdirec` UInt32,
	`txt` String
)
PRIMARY KEY material
SOURCE(POSTGRESQL(PORT 5432 HOST '192.168.214.203' USER 'std3_47' PASSWORD '5ZnVsbq48zvjW7bQ' DB 'adb' TABLE 'std3_47.product'))
LIFETIME(MIN 200 MAX 500)
LAYOUT(SPARSE_HASHED());

CREATE DICTIONARY std3_47.ch_region_dict
(
	`region` String,
	`txt` String
)
PRIMARY KEY region
SOURCE(POSTGRESQL(PORT 5432 HOST '192.168.214.203' USER 'std3_47' PASSWORD '5ZnVsbq48zvjW7bQ' DB 'adb' TABLE 'std3_47.region'))
LIFETIME(MIN 200 MAX 500)
LAYOUT(COMPLEX_KEY_HASHED());

DROP DICTIONARY std3_47.ch_price_dict;
DROP DICTIONARY std3_47.ch_chanel_dict;
DROP DICTIONARY std3_47.ch_product_dict;
DROP DICTIONARY std3_47.ch_region_dict;

CREATE TABLE std3_47.ch_plan_fact
(
	`region_code` String,
	`matdirec_code` String,
	`distr_chanel` UInt32,
	`planned_quantity` UInt32,
	`real_quantity` UInt32,
	`complete_percent` UInt32,
	`mosteff_material` String
)
ENGINE = ReplicatedMergeTree('/click/std3_47/ch_plan_fact/{shard}', '{replica}')
ORDER BY region_code
SETTINGS index_granularity = 8192;

CREATE TABLE std3_47.ch_plan_fact_distr
AS std3_47.ch_plan_fact
ENGINE = Distributed('default_cluster', 'std3_47', 'ch_plan_fact', complete_percent);

INSERT INTO std3_47.ch_plan_fact_distr
SELECT * FROM std3_47.ch_plan_fact_ext;

SELECT * FROM std3_47.ch_plan_fact_distr;
SELECT * FROM std3_47.ch_plan_fact;
