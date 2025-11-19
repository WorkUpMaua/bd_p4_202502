import os
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine, text


def run_sql_file(engine, path: Path) -> None:
    sql = path.read_text(encoding="utf-8")

    statements = [s.strip() for s in sql.split(";") if s.strip()]
    with engine.begin() as con:
        for stmt in statements:
            print(f"[ddl] executando statement:\n{stmt[:120]}...")
            con.execute(text(stmt))


def main() -> None:
    root_dir = Path(__file__).resolve().parents[1]

    # carrega variáveis do .env da raiz
    load_dotenv(root_dir / ".env")
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise SystemExit(
            "DATABASE_URL não definido no .env.\n"
            "Exemplo: DATABASE_URL=postgresql+psycopg2://user:pass@host:5432/seubanco"
        )

    print(f"[info] usando DATABASE_URL={db_url}")

    engine = create_engine(db_url, future=True, pool_pre_ping=True)

    schema_file = root_dir / "schemas" / "dw_schema.sql"
    if not schema_file.exists():
        raise SystemExit(f"Arquivo de schema dimensional não encontrado: {schema_file}")

    print(f"[info] aplicando schema dimensional a partir de: {schema_file}")
    run_sql_file(engine, schema_file)

    with engine.begin() as con:
        result = con.execute(
            text(
                """
                SELECT table_schema, table_name
                FROM information_schema.tables
                WHERE table_schema = 'dw'
                ORDER BY table_name;
                """
            )
        )
        tables = result.fetchall()

    print("[ok] tabelas no schema dw:")
    for schema, name in tables:
        print(f"  - {schema}.{name}")


if __name__ == "__main__":
    main()
