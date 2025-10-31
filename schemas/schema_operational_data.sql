CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS oltp;

CREATE TABLE IF NOT EXISTS staging.sales_raw (
  row_id        BIGINT,
  order_id      TEXT,
  order_date    DATE,
  ship_date     DATE,
  ship_mode     TEXT,
  customer_id   TEXT,
  customer_name TEXT,
  segment       TEXT,
  country       TEXT,
  city          TEXT,
  state         TEXT,
  postal_code   TEXT,
  region        TEXT,
  product_id    TEXT,
  category      TEXT,
  sub_category  TEXT,
  product_name  TEXT,
  sales         NUMERIC(14,2)
);

CREATE TABLE IF NOT EXISTS oltp.cliente (
  id_cliente     BIGSERIAL PRIMARY KEY,
  customer_id    TEXT UNIQUE NOT NULL,
  customer_name  TEXT,
  segment        TEXT,
  country        TEXT,
  city          TEXT,
  state          TEXT,
  postal_code    TEXT,
  region         TEXT
);

CREATE TABLE IF NOT EXISTS oltp.produto (
  id_produto     BIGSERIAL PRIMARY KEY,
  product_id     TEXT UNIQUE NOT NULL,
  product_name   TEXT,
  category       TEXT,
  sub_category   TEXT
);

CREATE TABLE IF NOT EXISTS oltp.pedido (
  id_pedido      BIGSERIAL PRIMARY KEY,
  order_id       TEXT UNIQUE NOT NULL,
  id_cliente     BIGINT NOT NULL REFERENCES oltp.cliente(id_cliente),
  order_date     DATE NOT NULL,
  ship_date      DATE,
  ship_mode      TEXT,
  created_at     TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS oltp.pedido_item (
  id_item        BIGSERIAL PRIMARY KEY,
  id_pedido      BIGINT NOT NULL REFERENCES oltp.pedido(id_pedido) ON DELETE CASCADE,
  id_produto     BIGINT NOT NULL REFERENCES oltp.produto(id_produto),
  quantidade     INT NOT NULL DEFAULT 1,
  preco_unitario NUMERIC(14,2) NOT NULL,
  valor_total    NUMERIC(14,2) GENERATED ALWAYS AS (quantidade * preco_unitario) STORED
);