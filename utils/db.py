import psycopg2
from psycopg2.extras import execute_batch
import pandas as pd
from utils.config import (
    PGHOST, PGDATABASE, PGUSER, PGPASSWORD,
    PGPORT, PGSSLMODE
)
from utils.logger import get_logger

logger = get_logger("db")


def get_conn():
    return psycopg2.connect(
        host=PGHOST,
        database=PGDATABASE,
        user=PGUSER,
        password=PGPASSWORD,
        port=PGPORT,
        sslmode=PGSSLMODE
    )


def ensure_table_exists():
    sql = """
    CREATE TABLE IF NOT EXISTS "Fato_Networks" (
        id SERIAL PRIMARY KEY,
        "Inep" TEXT,
        "Escola" TEXT,
        "TipoRede" TEXT,
        "Cidade" TEXT,
        "Estado" TEXT,
        "Devices" INTEGER,
        "Offline devices" INTEGER,
        "Offline_pct" FLOAT,
        "Clients" INTEGER,
        "Usage_GB" FLOAT,
        "Uso_por_AP_GB" FLOAT,
        "Clientes_por_AP" FLOAT,
        "Status_Utilizacao" TEXT,
        "Data_Coleta" TEXT
    );
    """
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()
    cur.close()
    conn.close()


def insert_into_db(df):
    sql = """
        INSERT INTO "Fato_Networks" (
            "Inep", "Escola", "TipoRede", "Cidade", "Estado",
            "Devices", "Offline devices", "Offline_pct",
            "Clients", "Usage_GB", "Uso_por_AP_GB",
            "Clientes_por_AP", "Status_Utilizacao", "Data_Coleta"
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
    """

    conn = get_conn()
    cur = conn.cursor()

    registros = [
        tuple(None if pd.isna(v) else v for v in row)
        for row in df.itertuples(index=False, name=None)
    ]

    execute_batch(cur, sql, registros)

    conn.commit()
    cur.close()
    conn.close()

    logger.info("🔥 Inserção finalizada!")
