-- 定义输入表
CREATE TABLE file_source(
  name STRING,
  age INT
)WITH(
  'connector' = 'filesystem',
  'path' = 'hdfs://bigdata01:9000/stu.json',
  'format' = 'json',
  'json.fail-on-missing-field' = 'false',
  'json.ignore-parse-errors' = 'true'
);


-- 定义输出表
CREATE TABLE file_sink(
  age INT,
  cnt BIGINT
)WITH(
  'connector' = 'filesystem',
  'path' = 'hdfs://bigdata01:9000/stu_out',
  'format' = 'json',
  'json.fail-on-missing-field' = 'false',
  'json.ignore-parse-errors' = 'true'
);

-- 指定批处理执行模式
SET 'execution.runtime-mode' = 'batch';

-- 指定任务名称
SET 'pipeline.name' = 'stu_job';

-- 设置任务并行度
SET 'parallelism.default' = '2';

-- SQL计算逻辑
INSERT INTO file_sink
SELECT
  age,
  COUNT(*) as cnt
FROM file_source
GROUP BY age;