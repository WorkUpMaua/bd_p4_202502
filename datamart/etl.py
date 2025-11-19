import os
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine, text


def run_step(con, label: str, sql: str) -> None:
    print(f"\n[ETL] Etapa: {label}")
    con.execute(text(sql))
    print(f"[ETL] Etapa '{label}' concluída.")


def main() -> None:
    root_dir = Path(__file__).resolve().parents[1]

    load_dotenv(root_dir / ".env")
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise SystemExit(
            "DATABASE_URL não definido no .env.\n"
            "Exemplo: DATABASE_URL=postgresql+psycopg2://user:pass@localhost:5432/seubanco"
        )

    print(f"[info] Usando DATABASE_URL={db_url}")

    engine = create_engine(db_url, future=True, pool_pre_ping=True)

    with engine.begin() as con:
        run_step(
            con,
            "truncate DW completo (fact + dims)",
            """
            TRUNCATE TABLE
              dw.fact_sales,
              dw.dim_ship_mode,
              dw.dim_date,
              dw.dim_product,
              dw.dim_customer
            RESTART IDENTITY CASCADE;
            """
        )

        sql_dim_ship_mode = """
            INSERT INTO dw.dim_ship_mode (ship_mode)
            SELECT DISTINCT NULLIF(TRIM(p.ship_mode), '')
            FROM oltp.pedido p
            WHERE NULLIF(TRIM(p.ship_mode), '') IS NOT NULL;
        """
        run_step(con, "carregar dim_ship_mode", sql_dim_ship_mode)

        sql_dim_customer = """
            INSERT INTO dw.dim_customer (
              customer_id,
              customer_name,
              segment,
              country,
              city,
              state,
              postal_code,
              region
            )
            SELECT
              c.customer_id,
              c.customer_name,
              c.segment,
              c.country,
              c.city,
              c.state,
              c.postal_code,
              c.region
            FROM oltp.cliente c;
        """
        run_step(con, "carregar dim_customer", sql_dim_customer)

        sql_dim_product = """
            INSERT INTO dw.dim_product (
              product_id,
              product_name,
              category,
              sub_category
            )
            SELECT
              p.product_id,
              p.product_name,
              p.category,
              p.sub_category
            FROM oltp.produto p;
        """
        run_step(con, "carregar dim_product", sql_dim_product)

        sql_dim_date = """
            WITH bounds AS (
              SELECT
                LEAST(MIN(order_date), MIN(ship_date)) AS dmin,
                GREATEST(MAX(order_date), MAX(ship_date)) AS dmax
              FROM oltp.pedido
            ),
            series AS (
              SELECT generate_series(dmin::date, dmax::date, interval '1 day')::date AS d
              FROM bounds
            )
            INSERT INTO dw.dim_date (
              date_sk,
              full_date,
              year,
              quarter,
              month,
              day,
              day_of_week,
              is_weekend
            )
            SELECT
              (EXTRACT(YEAR FROM s.d)::int * 10000 +
               EXTRACT(MONTH FROM s.d)::int * 100 +
               EXTRACT(DAY FROM s.d)::int) AS date_sk,
              s.d AS full_date,
              EXTRACT(YEAR    FROM s.d)::int AS year,
              EXTRACT(QUARTER FROM s.d)::int AS quarter,
              EXTRACT(MONTH   FROM s.d)::int AS month,
              EXTRACT(DAY     FROM s.d)::int AS day,
              EXTRACT(ISODOW  FROM s.d)::int AS day_of_week,     -- 1=Seg .. 7=Dom
              CASE WHEN EXTRACT(ISODOW FROM s.d)::int IN (6,7)
                   THEN TRUE ELSE FALSE END AS is_weekend
            FROM series s;
        """
        run_step(con, "carregar dim_date", sql_dim_date)

        sql_fact_sales = """
            INSERT INTO dw.fact_sales (
              customer_sk,
              product_sk,
              order_date_sk,
              ship_date_sk,
              ship_mode_sk,
              dd_order_id,
              sales
            )
            SELECT
              dc.customer_sk,
              dp.product_sk,
              (EXTRACT(YEAR FROM pe.order_date)::int * 10000 +
               EXTRACT(MONTH FROM pe.order_date)::int * 100 +
               EXTRACT(DAY FROM pe.order_date)::int) AS order_date_sk,
              CASE
                WHEN pe.ship_date IS NOT NULL
                THEN (EXTRACT(YEAR FROM pe.ship_date)::int * 10000 +
                      EXTRACT(MONTH FROM pe.ship_date)::int * 100 +
                      EXTRACT(DAY FROM pe.ship_date)::int)
                ELSE NULL
              END AS ship_date_sk,
              dsm.ship_mode_sk,
              pe.order_id AS dd_order_id,
              it.valor_total::NUMERIC(14,2) AS sales
            FROM oltp.pedido_item it
            JOIN oltp.pedido  pe ON pe.id_pedido   = it.id_pedido
            JOIN oltp.produto pr ON pr.id_produto  = it.id_produto
            JOIN oltp.cliente cl ON cl.id_cliente  = pe.id_cliente
            JOIN dw.dim_customer dc ON dc.customer_id = cl.customer_id
            JOIN dw.dim_product  dp ON dp.product_id  = pr.product_id
            LEFT JOIN dw.dim_ship_mode dsm
              ON dsm.ship_mode = NULLIF(TRIM(pe.ship_mode), '');
        """
        run_step(con, "carregar fact_sales", sql_fact_sales)

    with engine.begin() as con:
        counts = {}
        for table in [
            "dw.dim_customer",
            "dw.dim_product",
            "dw.dim_date",
            "dw.dim_ship_mode",
            "dw.fact_sales",
        ]:
            n = con.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()
            counts[table] = n

    print("\n[OK] Contagens após ETL:")
    for t, n in counts.items():
        print(f"  {t}: {n}")


if __name__ == "__main__":
    main()
