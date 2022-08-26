-- 窗口一个Kafka的Source表
CREATE TABLE tb_events (
  `user_id` BIGINT,
  `item_id` BIGINT,
  `behavior` STRING,
  `ts` TIMESTAMP(3) METADATA FROM 'timestamp'
) WITH (
  'connector' = 'kafka',
  'topic' = 'tp-events',
  'properties.bootstrap.servers' = 'node-1.51doit.cn:9092,node-2.51doit.cn:9092,node-3.51doit.cn:9092',
  'properties.group.id' = 'testGroup',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'csv',
  'csv.ignore-parse-errors' = 'true'
)
;
-- 创建一个print的Sink标签
CREATE TABLE print_table (
  `user_id` BIGINT,
  `item_id` BIGINT,
  `behavior` STRING,
  `ts` TIMESTAMP(3)
) WITH (
  'connector' = 'print'
)
;
-- 从Kafka的Source表中查询数据并过滤，然后出入到Sink表中
INSERT INTO print_table SELECT * FROM tb_events WHERE user_id IS NOT NULL AND behavior <> 'pay'


