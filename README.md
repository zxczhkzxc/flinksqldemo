# flinksql-submit

#### 配置
支持flink1.11 sql 语法
目前支持kafka mysql clickhouse Postgrasql数据源

### Sql配置
批量插入请加上BEGIN STATEMENT SET;  END;标识
```sql

CREATE TABLE ad_show (
adId string,
adName string,
adSource string,
adType string,
adUrl string,
clientNo string,
clientType string,
clientVersion string,
featureCode string,
fromUrl string,
ip string,
netType string,
osVersion string,
sessionId string,
sign string,
sn string,
userId string,
reqId string,
systemName string,
_lt string,
recommendtype  bigint,
ts as unix_time_convert(cast(_lt as bigint)),
WATERMARK FOR ts AS ts 
) WITH (
'connector' = 'kafka-0.11',
'topic' = 'ad_show',
'properties.bootstrap.servers' = '10.86.19.102:9092',
'properties.group.id' = 'testGroup',
'format' = 'json',
'scan.startup.mode' = 'earliest-offset'
);

CREATE TABLE ad_click (
adId string,
adName string,
adSource string,
adType string,
adUrl string,
clientNo string,
clientType string,
clientVersion string,
featureCode string,
fromUrl string,
ip string,
netType string,
osVersion string,
sessionId string,
sign string,
sn string,
userId string,
recommendtype bigint,
reqId string,
systemName string,
_lt string,
ts as unix_time_convert(cast(_lt as bigint)),
WATERMARK FOR ts AS ts

) WITH (
'connector' = 'kafka-0.11',
'topic' = 'ad_click',
'properties.bootstrap.servers' = '10.86.19.102:9092',
'properties.group.id' = 'testGroup',
'format' = 'json',
'scan.startup.mode' = 'earliest-offset'
);

CREATE TABLE rdwd_ad_show_click_i (
ad_id string , 
ad_name string , 
ad_source_code string ,
ad_position_code string ,
ad_url string ,
client_no string ,
client_type_id bigint , 
org_client_version string ,
client_version string ,
feature_code string,
from_url string ,
ip string ,
net_type string ,
os_version string ,
session_id string ,
sign string, 
sn string ,
user_id string ,
recommend_type bigint ,
req_id string ,
event_type bigint ,
event_date string,
gmt_create_time bigint,
dt string
) WITH (
'connector' = 'clickhouse',
'url' = 'jdbc:clickhouse://cdh14:8123/test',
'table-name' = 'rdwd_ad_show_click_i',
'username' = 'default',
'password' = 'admin',
'format' = 'json'
)
;

CREATE TABLE rdwd_ad_show_click_i_kafka (
ad_id string , 
ad_name string , 
ad_source_code string ,
ad_position_code string ,
ad_url string ,
client_no string ,
client_type_id bigint , 
org_client_version string ,
client_version string ,
feature_code string,
from_url string ,
ip string ,
net_type string ,
os_version string ,
session_id string ,
sign string, 
sn string ,
user_id string ,
recommend_type bigint ,
req_id string ,
event_type bigint ,
event_date string,
gmt_create_time bigint,
dt string
) WITH (
'connector' = 'kafka-0.11',
'topic' = 'rdwd_ad_show_click_i',
'properties.bootstrap.servers' = '10.86.19.102:9092',
'properties.group.id' = 'testGroup',
'format' = 'json'
)
;

CREATE view view_table as 
	SELECT 
	      adId AS ad_id,
         adName AS ad_name,
         adSource AS ad_source_code,
         adType AS ad_position_code,
         adUrl AS ad_url ,
         clientNo AS client_no,
         case when clientType is null or clientType ='' then -1 else cast(clientType as bigint) end AS client_type_id , 
         clientVersion AS org_client_version,
         substring(clientVersion,0,5) AS client_version ,
         featureCode AS feature_code,
         fromUrl AS from_url,
         ip,
         netType AS net_type ,
         osVersion AS os_version,
         sessionId AS session_id,
         sign,
         sn,
         userId AS user_id ,
         recommendtype AS recommend_type,
         reqId AS req_id,
         1 AS event_type, 
         data_format_local(cast(_lt as bigint)) AS event_date , 
         get_unix_time() as gmt_create_time,
         data_format_local(cast(_lt as bigint), 'yyyy-MM-dd') AS dt
FROM ad_show
UNION ALL
SELECT  
		 adId AS ad_id,
         adName AS ad_name,
         adSource AS ad_source_code,
         adType AS ad_position_code,
         adUrl AS ad_url ,
         clientNo AS client_no,
         case when clientType is null or clientType ='' then -1 else cast(clientType as bigint) end AS client_type_id ,
         clientVersion AS org_client_version,
         substring(clientVersion,0,5) AS client_version ,
         featureCode AS feature_code,
         fromUrl AS from_url,
         ip,
         netType AS net_type ,
         osVersion AS os_version,
         sessionId AS session_id,
         sign,
         sn,
         userId AS user_id ,
         recommendtype AS recommend_type,
         reqId AS req_id,
         2 AS event_type, 
         data_format_local(cast(_lt as bigint))  AS event_date , 
          get_unix_time() as gmt_create_time,
         data_format_local(cast(_lt as bigint), 'yyyy-MM-dd') AS dt
FROM ad_click
;


BEGIN STATEMENT SET; 
INSERT INTO rdwd_ad_show_click_i
SELECT * from view_table;

INSERT into rdwd_ad_show_click_i_kafka
SELECT * from view_table;
END;   
```

## 任务提交示例
```shell script
/opt/flink-1.11/bin/flink run -m yarn-cluster -yd  -p 3 -ys 3 -yjm 1024 -ytm 2048 -ynm stream_rdwd_ad_show_click_i_flink -c com.hikversion.flink.job.FlinkRun flinkTest-1.0.jar "/app/flinkJobSubmit/rdwd_ad_show_click_i.sql"

```

# 注：
代码在本地测试后打包必须设置checkpoint地址。在flink.properties中设置
checkpoints.dir=hdfs://***
