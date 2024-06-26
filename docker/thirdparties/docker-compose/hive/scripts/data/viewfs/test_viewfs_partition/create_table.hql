create database if not exists viewfs;

use viewfs;

CREATE TABLE `test_viewfs_partition`(
  `id` int,
  `name` string,
  `city` string,
  `age` int,
  `sex` string)
PARTITIONED BY (
  `part_col` int)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'viewfs://my-cluster/ns1/user/doris/suites/viewfs/partition_table';

msck repair table test_viewfs_partition;
