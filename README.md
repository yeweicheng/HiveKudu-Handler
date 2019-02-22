#Update 2019-02-22

# HiveKudu-Handler
Hive Kudu Storage Handler, Input & Output format, Writable and SerDe

This is the first release of Hive on Kudu.

I have placed the jars in the Resource folder which you can add in hive and test.

#### only support for exists kudu's table read and sql condition of key column must be defined

## Working Test case
### simple_test.sql
```sql
add jar async-1.4.1.jar;
add jar kudu-client-1.8.0.jar;
add jar KuduHandler-0.0.1.jar;
add jar kudu-mapreduce-1.8.0.jar;

set hive.cli.print.header=true;

CREATE EXTERNAL TABLE if not exists test_drop (
id INT,
name STRING
)
stored by 'org.apache.hadoop.hive.kududb.KuduHandler.KuduStorageHandler'
TBLPROPERTIES(
  'kudu.table_name' = 'test_drop',
  'kudu.master_addresses' = 'ip-172-31-56-74.ec2.internal:7051',
  'kudu.key_columns' = 'id'  -- comma separated column names to be used as primary key
);

describe formatted test_drop;

select count(*) from test_drop where id = 1;

select * from test_Drop where id = 1 and name = 'a';

select name, count(*) from test_drop where id = 1 group by name;

drop table test_Drop;
```
