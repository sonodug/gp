CREATE TABLE std3_47.report_202101
(
    `plant_id` Int32,
    `plant` String,
    `txt` String,
    `turnover` Int32,
    `coupon_discount` Decimal(17, 2),
    `turnover_with_discount` Decimal(17, 2),
    `material_qty` Int32,
    `bills_qty` Int32,
    `traffic` Int32,
    `matdisc_qty` Int32,
    `matdisc_percent` Decimal(7, 1),
    `avg_mat_qty` Decimal(7, 2),
    `conversion_rate` Decimal(7, 2),
    `avg_bill` Decimal(7, 1),
    `avg_profit` Decimal(7, 1)
)
ENGINE = ReplicatedMergeTree('/project/std3_47/report_202101/{shard}', '{replica}') 
ORDER BY plant;

CREATE TABLE std3_47.report_20210101
(

    `plant_id` Int32,

    `plant` String,

    `txt` String,

    `turnover` Int32,

    `coupon_discount` Decimal(17,
 2),

    `turnover_with_discount` Decimal(17,
 2),

    `material_qty` Int32,

    `bills_qty` Int32,

    `traffic` Int32,

    `matdisc_qty` Int32,

    `matdisc_percent` Decimal(7,
 1),

    `avg_mat_qty` Decimal(7,
 2),

    `conversion_rate` Decimal(7,
 2),

    `avg_bill` Decimal(7,
 1),

    `avg_profit` Decimal(7,
 1)
)
ENGINE = ReplicatedMergeTree('/project/std3_47/report_20210101/{shard}',
 '{replica}')
ORDER BY plant
SETTINGS index_granularity = 8192;

CREATE TABLE std3_47.report_20210101_20210105
(

    `plant_id` Int32,

    `plant` String,

    `txt` String,

    `turnover` Int32,

    `coupon_discount` Decimal(17,
 2),

    `turnover_with_discount` Decimal(17,
 2),

    `material_qty` Int32,

    `bills_qty` Int32,

    `traffic` Int32,

    `matdisc_qty` Int32,

    `matdisc_percent` Decimal(7,
 1),

    `avg_mat_qty` Decimal(7,
 2),

    `conversion_rate` Decimal(7,
 2),

    `avg_bill` Decimal(7,
 1),

    `avg_profit` Decimal(7,
 1)
)
ENGINE = ReplicatedMergeTree('/project/std3_47/report_20210101_20210105/{shard}',
 '{replica}')
ORDER BY plant
SETTINGS index_granularity = 8192;

CREATE TABLE std3_47.report_20210102
(

    `plant_id` Int32,

    `plant` String,

    `txt` String,

    `turnover` Int32,

    `coupon_discount` Decimal(17,
 2),

    `turnover_with_discount` Decimal(17,
 2),

    `material_qty` Int32,

    `bills_qty` Int32,

    `traffic` Int32,

    `matdisc_qty` Int32,

    `matdisc_percent` Decimal(7,
 1),

    `avg_mat_qty` Decimal(7,
 2),

    `conversion_rate` Decimal(7,
 2),

    `avg_bill` Decimal(7,
 1),

    `avg_profit` Decimal(7,
 1)
)
ENGINE = ReplicatedMergeTree('/project/std3_47/report_20210102/{shard}',
 '{replica}')
ORDER BY plant
SETTINGS index_granularity = 8192;

CREATE TABLE std3_47.report_20210103
(

    `plant_id` Int32,

    `plant` String,

    `txt` String,

    `turnover` Int32,

    `coupon_discount` Decimal(17,
 2),

    `turnover_with_discount` Decimal(17,
 2),

    `material_qty` Int32,

    `bills_qty` Int32,

    `traffic` Int32,

    `matdisc_qty` Int32,

    `matdisc_percent` Decimal(7,
 1),

    `avg_mat_qty` Decimal(7,
 2),

    `conversion_rate` Decimal(7,
 2),

    `avg_bill` Decimal(7,
 1),

    `avg_profit` Decimal(7,
 1)
)
ENGINE = ReplicatedMergeTree('/project/std3_47/report_20210103/{shard}',
 '{replica}')
ORDER BY plant
SETTINGS index_granularity = 8192;

CREATE TABLE std3_47.report_20210104
(

    `plant_id` Int32,

    `plant` String,

    `txt` String,

    `turnover` Int32,

    `coupon_discount` Decimal(17,
 2),

    `turnover_with_discount` Decimal(17,
 2),

    `material_qty` Int32,

    `bills_qty` Int32,

    `traffic` Int32,

    `matdisc_qty` Int32,

    `matdisc_percent` Decimal(7,
 1),

    `avg_mat_qty` Decimal(7,
 2),

    `conversion_rate` Decimal(7,
 2),

    `avg_bill` Decimal(7,
 1),

    `avg_profit` Decimal(7,
 1)
)
ENGINE = ReplicatedMergeTree('/project/std3_47/report_20210104/{shard}',
 '{replica}')
ORDER BY plant
SETTINGS index_granularity = 8192;

CREATE TABLE std3_47.report_202102
(

    `plant_id` Int32,

    `plant` String,

    `txt` String,

    `turnover` Int32,

    `coupon_discount` Decimal(17,
 2),

    `turnover_with_discount` Decimal(17,
 2),

    `material_qty` Int32,

    `bills_qty` Int32,

    `traffic` Int32,

    `matdisc_qty` Int32,

    `matdisc_percent` Decimal(7,
 1),

    `avg_mat_qty` Decimal(7,
 2),

    `conversion_rate` Decimal(7,
 2),

    `avg_bill` Decimal(7,
 1),

    `avg_profit` Decimal(7,
 1)
)
ENGINE = ReplicatedMergeTree('/project/std3_47/report_202102/{shard}',
 '{replica}')
ORDER BY plant
SETTINGS index_granularity = 8192;

CREATE TABLE std3_47.report_202101_202103
(

    `plant_id` Int32,

    `plant` String,

    `txt` String,

    `turnover` Int32,

    `coupon_discount` Decimal(17,
 2),

    `turnover_with_discount` Decimal(17,
 2),

    `material_qty` Int32,

    `bills_qty` Int32,

    `traffic` Int32,

    `matdisc_qty` Int32,

    `matdisc_percent` Decimal(7,
 1),

    `avg_mat_qty` Decimal(7,
 2),

    `conversion_rate` Decimal(7,
 2),

    `avg_bill` Decimal(7,
 1),

    `avg_profit` Decimal(7,
 1)
)
ENGINE = ReplicatedMergeTree('/project/std3_47/report_202101_202103/{shard}',
 '{replica}')
ORDER BY plant
SETTINGS index_granularity = 8192;