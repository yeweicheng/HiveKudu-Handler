#Update 2019-02-22

# HiveKudu-Handler
Hive Kudu Storage Handler, Input & Output format, Writable and SerDe

#### only support for exists kudu's table read and sql condition of key column must be defined

## Working Test case
### simple_test.sql
```sql
add jar async-1.4.1.jar;
add jar kudu-client-1.8.0.jar;
add jar KuduHandler-0.0.1.jar;
add jar kudu-mapreduce-1.8.0.jar;

CREATE EXTERNAL TABLE if not exists test_drop (
id INT,
name STRING
)
stored by 'org.apache.hadoop.hive.kududb.KuduHandler.KuduStorageHandler'
TBLPROPERTIES(
  'kudu.table_name' = 'test_drop',
  'kudu.master_addresses' = 'localhost:7051',
  'kudu.key_columns' = 'id'
);

select count(*) from test_drop where id = 1;

select * from test_Drop where id = 1 and name = 'a';

select name, count(*) from test_drop where id = 1 group by name;

```
