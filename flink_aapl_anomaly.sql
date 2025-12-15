SET 'execution.runtime-mode' = 'streaming';
SET 'execution.attached' = 'true';

-- Source: đọc dữ liệu giá AAPL từ Kafka
CREATE TABLE aapl_quotes (
    symbol STRING,
    price DOUBLE,
    open_price DOUBLE,
    price_change DOUBLE,
    pct_change DOUBLE,
    high DOUBLE,
    low DOUBLE,
    prev_close DOUBLE,
    time_unix BIGINT,
    time_iso STRING,
    ts AS TO_TIMESTAMP_LTZ(time_unix * 1000, 3),
    WATERMARK FOR ts AS ts - INTERVAL '10' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'aapl_quotes',
    'properties.bootstrap.servers' = 'kafka:9093',
    'properties.group.id' = 'flink-aapl-postgres',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json'
);

-- Sink 1: PostgreSQL
CREATE TABLE postgres_sink (
    symbol STRING,
    price DOUBLE,
    pct_change DOUBLE,
    time_iso STRING,
    event_time TIMESTAMP
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/stocks',
    'table-name' = 'aapl_prices',
    'username' = 'admin',
    'password' = 'admin123',
    'driver' = 'org.postgresql.Driver',
    'sink.buffer-flush.max-rows' = '10',
    'sink.buffer-flush.interval' = '5s'
);

-- Sink 2: Elasticsearch
CREATE TABLE es_aapl_prices (
    symbol STRING,
    price DOUBLE,
    pct_change DOUBLE,
    time_iso STRING,
    event_time TIMESTAMP(3),
    PRIMARY KEY (symbol, event_time) NOT ENFORCED
) WITH (
    'connector' = 'elasticsearch-7',
    'hosts' = 'http://elasticsearch:9200',
    'index' = 'aapl_prices'
    -- với ES7 không cần khai báo 'document-type', mặc định là "_doc"
);

-- Ghi vào PostgreSQL
INSERT INTO postgres_sink
SELECT
    symbol,
    price,
    pct_change,
    time_iso,
    CAST(ts AS TIMESTAMP)
FROM aapl_quotes
WHERE pct_change IS NOT NULL;

-- Ghi vào Elasticsearch (cùng dữ liệu như Postgres)
INSERT INTO es_aapl_prices
SELECT
    symbol,
    price,
    pct_change,
    time_iso,
    CAST(ts AS TIMESTAMP)
FROM aapl_quotes
WHERE pct_change IS NOT NULL;
