from __future__ import annotations

import pandas as pd
from prefect import flow, task, get_run_logger
from sqlalchemy import create_engine, text

# Conexión a tu RDS (PostgreSQL)
AWS_URL = "postgresql+psycopg2://postgres:password@tallercloud.c9ujhynkiloz.us-east-1.rds.amazonaws.com:5432/odoo"

# Tabla origen (la que cargaste en el Ej. 7)
SOURCE_TABLE = "ejercicio5_ventas_facturas"

# Tabla destino (snapshot histórico para evidenciar cambios)
DEST_TABLE = "ejercicio5_snapshot"


@task
def extract_from_rds() -> pd.DataFrame:
    logger = get_run_logger()
    engine = create_engine(AWS_URL)
    with engine.connect() as conn:
        df = pd.read_sql_query(text(f"SELECT * FROM {SOURCE_TABLE};"), conn)
    logger.info(f"Extract: {len(df)} filas desde {SOURCE_TABLE}")
    return df


@task
def transform(df: pd.DataFrame) -> pd.DataFrame:
    # Normaliza nulos (NaN/NaT) a None para que Postgres acepte NULL
    df2 = df.copy()
    df2 = df2.astype(object).where(pd.notna(df2), None)
    return df2


@task
def ensure_snapshot_table() -> None:
    """
    Crea la tabla de snapshots si no existe.
    Incluye run_ts para identificar cada corrida y snapshot_id como PK.
    """
    engine = create_engine(AWS_URL)
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {DEST_TABLE} (
      snapshot_id BIGSERIAL PRIMARY KEY,
      run_ts TIMESTAMP NOT NULL DEFAULT NOW(),
      numero_orden TEXT,
      cliente TEXT,
      fecha_orden DATE,
      total_orden NUMERIC,
      invoice_status TEXT,
      numero_factura TEXT,
      invoice_date DATE,
      total_factura NUMERIC,
      corresponde_factura TEXT
    );
    """
    with engine.connect() as conn:
        conn = conn.execution_options(isolation_level="AUTOCOMMIT")
        conn.execute(text(create_sql))


@task
def load_snapshot(df: pd.DataFrame) -> int:
    """
    Inserta un snapshot nuevo en DEST_TABLE copiando el estado actual de SOURCE_TABLE.
    NOTA: No truncamos DEST_TABLE para poder comparar corridas.
    """
    logger = get_run_logger()
    engine = create_engine(AWS_URL)

    # Asegura la tabla destino
    ensure_snapshot_table()

    # Insert snapshot (copiando desde la tabla origen)
    insert_sql = f"""
    INSERT INTO {DEST_TABLE} (
      numero_orden, cliente, fecha_orden, total_orden, invoice_status,
      numero_factura, invoice_date, total_factura, corresponde_factura
    )
    SELECT
      numero_orden, cliente, fecha_orden, total_orden, invoice_status,
      numero_factura, invoice_date, total_factura, corresponde_factura
    FROM {SOURCE_TABLE};
    """
    with engine.connect() as conn:
        conn = conn.execution_options(isolation_level="AUTOCOMMIT")
        result = conn.execute(text(insert_sql))

    logger.info(f"Load: snapshot insert realizado (rowcount={result.rowcount})")
    return int(result.rowcount or 0)


@task
def check_latest(limit: int = 50) -> pd.DataFrame:
    """
    Trae las filas del snapshot más reciente (último run_ts).
    """
    engine = create_engine(AWS_URL)
    q = f"""
    SELECT *
    FROM {DEST_TABLE}
    WHERE run_ts = (SELECT MAX(run_ts) FROM {DEST_TABLE})
    ORDER BY snapshot_id
    LIMIT {limit};
    """
    with engine.connect() as conn:
        df = pd.read_sql_query(text(q), conn)
    return df


@flow(name="etl_rds_to_rds_snapshot")
def etl_rds_to_rds_snapshot():
    logger = get_run_logger()

    df = extract_from_rds()
    df2 = transform(df)

    inserted = load_snapshot(df2)
    logger.info(f"Filas insertadas en snapshot: {inserted}")

    df_latest = check_latest()
    if "corresponde_factura" in df_latest.columns and not df_latest.empty:
        logger.info(
            f"Resumen última corrida: {df_latest['corresponde_factura'].value_counts(dropna=False).to_dict()}"
        )
    else:
        logger.info("No se encontraron filas en el snapshot más reciente (tabla vacía o sin datos).")

    return df_latest
