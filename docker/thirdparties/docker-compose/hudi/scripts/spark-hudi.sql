create table if not exists partitioned_mor (
    `rowid` string,
    `precomb` bigint,
    `name` string,
    `tobedeletedstr` string,
    `inttolong` int,
    `longtoint` bigint,
    `partitionid` string,
    `versionid` string
) using hudi
tblproperties (
    type = 'mor',
    primaryKey = 'rowid',
    preCombineField = 'precomb'
) partitioned by (partitionid, versionid);

insert into partitioned_mor values
("row1", 1, "name1", "tobe1", 1, 1001, "part-0", 1),
("row2", 2, "name2", "tobe2", 2, 1002, "part-0", 2),
("row3", 3, "name3", "tobe3", 3, 1003, "part-1", 3),
("row4", 4, "name4", "tobe4", 4, 1004, "part-1", 4),
("row5", 5, "name5", "tobe5", 5, 1005, "part-1", 5),
("row6", 6, "name6", "tobe6", 6, 1006, "part-0", 1),
("row7", 7, "name7", "tobe7", 7, 1007, "part-0", 2);


create table test_hudi(
    `rowid` string,
    `precomb` bigint,
    `name` string
) using hudi
tblproperties (
    type = 'mor',
    primaryKey = 'rowid',
    preCombineField = 'precomb'
);

insert into test_hudi values
("row1", 1, "name1"),
("row2", 2, "name2");