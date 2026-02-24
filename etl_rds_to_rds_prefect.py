from __future__ import annotations

import pandas as pd
from prefect import flow, task, get_run_logger
from sqlalchemy import create_engine, text

AWS_URL = "postgresql+psycopg2://postgres:password@tallercloud.c9ujhynkiloz.us-east-1.rds.amazonaws.com:5432/odoo"

SOURCE_TABLE = "ejercicio5_ventas_facturas"
DEST_TABLE   = "ejercicio5_snapshot"

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
    # en este caso casi no transformamos, solo aseguramos nulos correctos
    df2 = df.copy()
    df2 = df2.astype(object).where(pd.notna(df2), None)
    return df2

@task
def load_snapshot(df: pd.DataFrame) -> int:
    logger = get_run_logger()
    engine = create_engine(AWS_URL)

    # Insertamos un snapshot nuevo (NO truncamos, porque queremos evidenciar cambios entre ejecuciones)
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
    # result.rowcount suele devolver filas insertadas
    logger.info(f"Load: snapshot insert realizado (rowcount={result.rowcount})")
    return int(result.rowcount or 0)

@task
def check_latest() -> pd.DataFrame:
    engine = create_engine(AWS_URL)
    q = f"""
    SELECT *
    FROM {DEST_TABLE}
    WHERE run_ts = (SELECT MAX(run_ts) FROM {DEST_TABLE})
    ORDER BY snapshot_id
    LIMIT 50;
    """
    with engine.connect() as conn:
        df = pd.read_sql_query(text(q), conn)
    return df

@flow(name="etl_rds_to_rds_snapshot")
def etl_rds_to_rds_snapshot():
    df = extract_from_rds()
    df2 = transform(df)
    load_snapshot(df2)
    df_latest = check_latest()
    logger = get_run_logger()
    if "corresponde_factura" in df_latest.columns:
        logger.info(f"Resumen última corrida: {df_latest['corresponde_factura'].value_counts(dropna=False).to_dict()}")
    return df_latest