CREATE SCHEMA IF NOT EXISTS hive.sf10_parquet;

USE hive.sf10_parquet;

CREATE TABLE IF NOT EXISTS customer (
   c_custkey bigint,
   c_name varchar(25),
   c_address varchar(40),
   c_nationkey bigint,
   c_phone varchar(15),
   c_acctbal double,
   c_mktsegment varchar(10),
   c_comment varchar(117)
)
WITH (format = 'PARQUET', external_location = 'file:///prestissimo/velox/velox-tpch-sf10-2-data/customer.d');

CREATE TABLE IF NOT EXISTS lineitem (
  l_orderkey bigint,
  l_partkey bigint,
  l_suppkey bigint,
  l_linenumber bigint,
  l_quantity double,
  l_extendedprice double,
  l_discount double,
  l_tax double,
  l_returnflag varchar(1),
  l_linestatus varchar(1),
  l_shipdate date,
  l_commitdate date,
  l_receiptdate date,
  l_shipinstruct varchar(25),
  l_shipmode varchar(10),
  l_comment varchar(44)
)
WITH (format = 'PARQUET', external_location = 'file:///prestissimo/velox/velox-tpch-sf10-2-data/lineitem.d');

CREATE TABLE IF NOT EXISTS orders (
  o_orderkey bigint,
  o_custkey bigint,
  o_orderstatus varchar(1),
  o_totalprice double,
  o_orderdate date,
  o_orderpriority varchar(15),
  o_clerk varchar(15),
  o_shippriority bigint,
  o_comment varchar(79)
)
WITH (format = 'PARQUET', external_location = 'file:///prestissimo/velox/velox-tpch-sf10-2-data/orders.d');

CREATE TABLE IF NOT EXISTS nation (
  n_nationkey bigint,
  n_name varchar(25),
  n_regionkey bigint,
  n_comment varchar(152))
WITH (format = 'PARQUET', external_location = 'file:///prestissimo/velox/velox-tpch-sf10-2-data/nation.d');

CREATE TABLE IF NOT EXISTS region (
  r_regionkey bigint,
  r_name varchar(25),
  r_comment varchar(152)
)
WITH (format = 'PARQUET', external_location = 'file:///prestissimo/velox/velox-tpch-sf10-2-data/region.d');

CREATE TABLE IF NOT EXISTS part (
  p_partkey bigint,
  p_name varchar(55),
  p_mfgr varchar(25),
  p_brand varchar(10),
  p_type varchar(25),
  p_size bigint,
  p_container varchar(10),
  p_retailprice double,
  p_comment varchar(23)
)
WITH (format = 'PARQUET', external_location = 'file:///prestissimo/velox/velox-tpch-sf10-2-data/part.d');

CREATE TABLE IF NOT EXISTS supplier (
  s_suppkey bigint,
  s_name varchar(25),
  s_address varchar(40),
  s_nationkey bigint,
  s_phone varchar(15),
  s_acctbal double,
  s_comment varchar(101)
)
WITH (format = 'PARQUET', external_location = 'file:///prestissimo/velox/velox-tpch-sf10-2-data/supplier.d');

CREATE TABLE IF NOT EXISTS partsupp (
  ps_partkey bigint,
  ps_suppkey bigint,
  ps_availqty double,
  ps_supplycost double,
  ps_comment varchar(199)
)
WITH (format = 'PARQUET', external_location = 'file:///prestissimo/velox/velox-tpch-sf10-2-data/partsupp.d');
