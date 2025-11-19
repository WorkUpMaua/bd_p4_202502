CREATE SCHEMA IF NOT EXISTS dw;

CREATE TABLE IF NOT EXISTS dw.dim_customer (
  customer_sk   BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  customer_id   TEXT   NOT NULL UNIQUE, 
  customer_name TEXT,
  segment       TEXT,
  country       TEXT,
  city          TEXT,
  state         TEXT,
  postal_code   TEXT,
  region        TEXT
);

CREATE TABLE IF NOT EXISTS dw.dim_product (
  product_sk   BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  product_id   TEXT   NOT NULL UNIQUE, 
  product_name TEXT,
  category     TEXT,
  sub_category TEXT
);

CREATE TABLE IF NOT EXISTS dw.dim_date (
  date_sk     INTEGER PRIMARY KEY,      
  full_date   DATE    NOT NULL UNIQUE,
  year        INTEGER NOT NULL,
  quarter     INTEGER NOT NULL,
  month       INTEGER NOT NULL,
  day         INTEGER NOT NULL,
  day_of_week INTEGER NOT NULL,         
  is_weekend  BOOLEAN NOT NULL
);


CREATE TABLE IF NOT EXISTS dw.dim_ship_mode (
  ship_mode_sk BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  ship_mode    TEXT NOT NULL UNIQUE
);


CREATE TABLE IF NOT EXISTS dw.fact_sales (
  fact_id        BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  customer_sk    BIGINT NOT NULL REFERENCES dw.dim_customer(customer_sk),
  product_sk     BIGINT NOT NULL REFERENCES dw.dim_product(product_sk),
  order_date_sk  INTEGER NOT NULL REFERENCES dw.dim_date(date_sk),
  ship_date_sk   INTEGER     REFERENCES dw.dim_date(date_sk),
  ship_mode_sk   BIGINT      REFERENCES dw.dim_ship_mode(ship_mode_sk),
  dd_order_id    TEXT,
  sales          NUMERIC(14,2) NOT NULL
);
