CREATE SCHEMA IF NOT EXISTS hive.sf1_zoltan_parquet;

USE hive.sf1_zoltan_parquet;

CREATE TABLE IF NOT EXISTS customer (
   custkey bigint,
   name varchar(25),
   address varchar(40),
   nationkey bigint,
   phone varchar(15),
   acctbal double,
   mktsegment varchar(10),
   comment varchar(117)
)
WITH (format = 'PARQUET', external_location = 'file:///prestissimo/velox/velox-tpch-sf1-zoltan-data/customer');

CREATE TABLE IF NOT EXISTS lineitem (
  orderkey bigint,
  partkey bigint,
  suppkey bigint,
  linenumber integer,
  quantity double,
  extendedprice double,
  discount double,
  tax double,
  returnflag varchar(1),
  linestatus varchar(1),
  shipdate date,
  commitdate date,
  receiptdate date,
  shipinstruct varchar(25),
  shipmode varchar(10),
  comment varchar(44)
)
WITH (format = 'PARQUET', external_location = 'file:///prestissimo/velox/velox-tpch-sf1-zoltan-data/lineitem');

CREATE TABLE IF NOT EXISTS orders (
  orderkey bigint,
  custkey bigint,
  orderstatus varchar(1),
  totalprice double,
  orderdate date,
  orderpriority varchar(15),
  clerk varchar(15),
  shippriority integer,
  comment varchar(79)
)
WITH (format = 'PARQUET', external_location = 'file:///prestissimo/velox/velox-tpch-sf1-zoltan-data/orders');

CREATE TABLE IF NOT EXISTS nation (
  nationkey bigint,
  name varchar(25),
  regionkey bigint,
  comment varchar(152))
WITH (format = 'PARQUET', external_location = 'file:///prestissimo/velox/velox-tpch-sf1-zoltan-data/nation');

CREATE TABLE IF NOT EXISTS region (
  regionkey bigint,
  name varchar(25),
  comment varchar(152)
)
WITH (format = 'PARQUET', external_location = 'file:///prestissimo/velox/velox-tpch-sf1-zoltan-data/region');

CREATE TABLE IF NOT EXISTS part (
  partkey bigint,
  name varchar(55),
  mfgr varchar(25),
  brand varchar(10),
  type varchar(25),
  size integer,
  container varchar(10),
  retailprice double,
  comment varchar(23)
)
WITH (format = 'PARQUET', external_location = 'file:///prestissimo/velox/velox-tpch-sf1-zoltan-data/part');

CREATE TABLE IF NOT EXISTS supplier (
  suppkey bigint,
  name varchar(25),
  address varchar(40),
  nationkey bigint,
  phone varchar(15),
  acctbal double,
  comment varchar(101)
)
WITH (format = 'PARQUET', external_location = 'file:///prestissimo/velox/velox-tpch-sf1-zoltan-data/supplier');

CREATE TABLE IF NOT EXISTS partsupp (
  partkey bigint,
  suppkey bigint,
  availqty integer,
  supplycost double,
  comment varchar(199)
)
WITH (format = 'PARQUET', external_location = 'file:///prestissimo/velox/velox-tpch-sf1-zoltan-data/partsupp');
