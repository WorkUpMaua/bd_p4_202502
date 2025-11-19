import argparse
from pathlib import Path
import zipfile
import subprocess
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

import datasource 

def run(cmd):
    print("$", " ".join(cmd))
    subprocess.run(cmd, check=True)

def maybe_download_kaggle(dataset: str, workdir: Path) -> Path:
    workdir.mkdir(parents=True, exist_ok=True)
    run(["kaggle", "datasets", "download", "-d", dataset, "-p", str(workdir)])
    zips = sorted(workdir.glob("*.zip"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not zips:
        raise FileNotFoundError("Nenhum .zip encontrado após baixar do Kaggle.")
    return zips[0]

def extract_zip(zip_path: Path, dest: Path) -> list[Path]:
    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(dest)
        names = zf.namelist()
    return [dest / n for n in names if not n.endswith("/")]

def execute_sql_file(engine, sql_path: Path):
    with open(sql_path, "r", encoding="utf-8") as f:
        sql = f.read()
    with engine.begin() as con:
        for stmt in [s.strip() for s in sql.split(";") if s.strip()]:
            con.execute(text(stmt))

def load_csv_to_staging(engine, csv_path: Path, staging_table: str = "sales_raw"):
    dtype = {
        "Row ID": "Int64",
        "Order ID": "string",
        "Ship Mode": "string",
        "Customer ID": "string",
        "Customer Name": "string",
        "Segment": "string",
        "Country": "string",
        "City": "string",
        "State": "string",
        "Postal Code": "string",
        "Region": "string",
        "Product ID": "string",
        "Category": "string",
        "Sub-Category": "string",
        "Product Name": "string",
        "Sales": "float",
    }
    parse_dates = ["Order Date", "Ship Date"]
    df = pd.read_csv(csv_path, dtype=dtype, parse_dates=parse_dates, dayfirst=False)

    rename_map = {
        "Row ID":"row_id", "Order ID":"order_id", "Order Date":"order_date", "Ship Date":"ship_date", "Ship Mode":"ship_mode",
        "Customer ID":"customer_id", "Customer Name":"customer_name", "Segment":"segment", "Country":"country",
        "City":"city", "State":"state", "Postal Code":"postal_code", "Region":"region", "Product ID":"product_id",
        "Category":"category", "Sub-Category":"sub_category", "Product Name":"product_name", "Sales":"sales"
    }
    df = df.rename(columns=rename_map)
    if "order_date" in df.columns:
        df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")
    if "ship_date" in df.columns:
        df["ship_date"] = pd.to_datetime(df["ship_date"], errors="coerce")

    df.to_sql(staging_table, engine, schema="staging",
              if_exists="append", index=False, method="multi", chunksize=10000)
    print(f"[ok] staging.{staging_table} carregada com {len(df):,} linhas")

def main():
    load_dotenv()

    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", type=str, default="", help="Caminho do CSV local")
    parser.add_argument("--schema-sql", type=str, default="schema_operational_data.sql", help="Arquivo SQL de criação de tabelas")
    parser.add_argument("--staging-table", type=str, default="sales_raw")
    parser.add_argument("--no-download", action="store_true", help="Não usar Kaggle; exige --csv")
    args = parser.parse_args()

    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise SystemExit("Defina DATABASE_URL no .env (ex.: postgresql+psycopg2://user:pass@host:5432/db)")

    engine = create_engine(db_url, pool_pre_ping=True, future=True)

    csv_path = None
    if not args.no_download:
        dataset = os.getenv("KAGGLE_DATASET", "")
        kfile = os.getenv("KAGGLE_FILE", "")
        if not dataset and not args.csv:
            raise SystemExit("Defina KAGGLE_DATASET no .env ou passe --csv PATH e use --no-download.")
        if dataset:
            work = Path("data")
            zip_path = maybe_download_kaggle(dataset, work)
            files = extract_zip(zip_path, work)
            if kfile:
                csv_candidate = work / kfile
                if not csv_candidate.exists():
                    raise SystemExit(f"KAGGLE_FILE não encontrado: {csv_candidate}")
                csv_path = csv_candidate
            else:
                csvs = [p for p in files if p.suffix.lower() == ".csv"]
                if not csvs:
                    raise SystemExit("Nenhum CSV encontrado no ZIP. Defina KAGGLE_FILE no .env.")
                csvs.sort(key=lambda p: p.stat().st_size, reverse=True)
                csv_path = csvs[0]
    else:
        if not args.csv:
            raise SystemExit("Use --csv PATH quando --no-download for usado.")
        csv_path = Path(args.csv)
        if not csv_path.exists():
            raise SystemExit(f"CSV não encontrado: {csv_path}")

    sql_path = Path("schemas/" + args.schema_sql)
    if not sql_path.exists():
        raise SystemExit(f"Arquivo SQL não encontrado: {sql_path}")
    execute_sql_file(engine, sql_path)

    load_csv_to_staging(engine, csv_path, staging_table=args.staging_table)

    with engine.begin() as con:
        n = con.execute(text("SELECT COUNT(*) FROM staging.sales_raw")).scalar()
        print(f"[info] staging.sales_raw linhas = {n}")

    datasource.normalize_all(engine, staging_table=args.staging_table)
    print("[done] Pipeline concluído com sucesso.")

if __name__ == "__main__":
    main()
