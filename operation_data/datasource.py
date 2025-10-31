from sqlalchemy import text

def insert_clientes(engine, staging_table="sales_raw"):
    sql = f"""
    WITH src AS (
      SELECT
        REGEXP_REPLACE(TRIM(s.customer_id), '\\s+', '', 'g') AS customer_id,
        NULLIF(TRIM(s.customer_name), '') AS customer_name,
        NULLIF(TRIM(s.segment), '')       AS segment,
        NULLIF(TRIM(s.country), '')       AS country,
        NULLIF(TRIM(s.city), '')          AS city,
        NULLIF(TRIM(s.state), '')         AS state,
        NULLIF(TRIM(s.postal_code), '')   AS postal_code,
        NULLIF(TRIM(s.region), '')        AS region
      FROM staging.{staging_table} s
      WHERE s.customer_id IS NOT NULL
    ),
    dedup AS (
      -- deduplica por chave de negócio (customer_id)
      SELECT
        customer_id,
        MAX(customer_name) AS customer_name,
        MAX(segment)       AS segment,
        MAX(country)       AS country,
        MAX(city)          AS city,
        MAX(state)         AS state,
        MAX(postal_code)   AS postal_code,
        MAX(region)        AS region
      FROM src
      GROUP BY customer_id
    )
    INSERT INTO oltp.cliente (customer_id, customer_name, segment, country, city, state, postal_code, region)
    SELECT * FROM dedup
    ON CONFLICT (customer_id) DO UPDATE
      SET customer_name = EXCLUDED.customer_name,
          segment       = EXCLUDED.segment,
          country       = EXCLUDED.country,
          city          = EXCLUDED.city,
          state         = EXCLUDED.state,
          postal_code   = EXCLUDED.postal_code,
          region        = EXCLUDED.region;
    """
    with engine.begin() as con:
        con.execute(text(sql))
        n = con.execute(text("SELECT COUNT(*) FROM oltp.cliente")).scalar()
        print(f"[ok] oltp.cliente total = {n}")

def insert_produtos(engine, staging_table="sales_raw"):
    sql = f"""
    WITH src AS (
      SELECT
        REGEXP_REPLACE(TRIM(s.product_id), '\\s+', '', 'g') AS product_id,
        NULLIF(TRIM(s.product_name), '') AS product_name,
        NULLIF(TRIM(s.category), '')     AS category,
        NULLIF(TRIM(s.sub_category), '') AS sub_category
      FROM staging.{staging_table} s
      WHERE s.product_id IS NOT NULL
    ),
    dedup AS (
      SELECT
        product_id,
        MAX(product_name) AS product_name,
        MAX(category)     AS category,
        MAX(sub_category) AS sub_category
      FROM src
      GROUP BY product_id
    )
    INSERT INTO oltp.produto (product_id, product_name, category, sub_category)
    SELECT * FROM dedup
    ON CONFLICT (product_id) DO UPDATE
      SET product_name = EXCLUDED.product_name,
          category     = EXCLUDED.category,
          sub_category = EXCLUDED.sub_category;
    """
    with engine.begin() as con:
        con.execute(text(sql))
        n = con.execute(text("SELECT COUNT(*) FROM oltp.produto")).scalar()
        print(f"[ok] oltp.produto total = {n}")

def insert_pedidos(engine, staging_table="sales_raw"):
    sql = f"""
    INSERT INTO oltp.pedido (order_id, id_cliente, order_date, ship_date, ship_mode)
    SELECT DISTINCT
      TRIM(s.order_id)                             AS order_id,
      c.id_cliente                                 AS id_cliente,
      s.order_date,
      s.ship_date,
      NULLIF(TRIM(s.ship_mode), '')                AS ship_mode
    FROM staging.{staging_table} s
    JOIN oltp.cliente c
      ON c.customer_id = REGEXP_REPLACE(TRIM(s.customer_id), '\\s+', '', 'g')
    WHERE s.order_id IS NOT NULL
      AND s.order_date IS NOT NULL
    ON CONFLICT (order_id) DO NOTHING;
    """
    with engine.begin() as con:
        con.execute(text(sql))
        n = con.execute(text("SELECT COUNT(*) FROM oltp.pedido")).scalar()
        print(f"[ok] oltp.pedido total = {n}")

def insert_pedido_itens(engine, staging_table="sales_raw"):
    sql = f"""
    INSERT INTO oltp.pedido_item (id_pedido, id_produto, quantidade, preco_unitario)
    SELECT
      p.id_pedido,
      pr.id_produto,
      1,
      COALESCE(s.sales, 0)::numeric(14,2)
    FROM staging.{staging_table} s
    JOIN oltp.pedido  p  ON p.order_id    = TRIM(s.order_id)
    JOIN oltp.produto pr ON pr.product_id = REGEXP_REPLACE(TRIM(s.product_id), '\\s+', '', 'g');
    """
    with engine.begin() as con:
        con.execute(text(sql))
        n = con.execute(text("SELECT COUNT(*) FROM oltp.pedido_item")).scalar()
        print(f"[ok] oltp.pedido_item total = {n}")

def normalize_all(engine, staging_table="sales_raw"):
    insert_clientes(engine, staging_table)
    insert_produtos(engine, staging_table)
    insert_pedidos(engine, staging_table)
    insert_pedido_itens(engine, staging_table)
