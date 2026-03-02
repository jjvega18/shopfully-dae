import polars as pl
from db import PgConfig, get_conn
import os

RAW_COLS = ["visit_id", "user_id", "store_id", "store_cat", "city", "timestamp", "duration_s"]

SQL_IS_EXACT_DUP = """
SELECT 1
FROM "RawStoreVisits"
WHERE "visit_id"   = %s
  AND "user_id"    = %s
  AND "store_id"   = %s
  AND "store_cat"  = %s
  AND "city"       = %s
  AND "timestamp"  = %s
  AND "duration_s" = %s
LIMIT 1
"""

SQL_INSERT_RAW = """
INSERT INTO "RawStoreVisits"
("visit_id","user_id","store_id","store_cat","city","timestamp","duration_s","ingested_at")
VALUES (%s,%s,%s,%s,%s,%s,%s, NOW()::timestamp)
ON CONFLICT ("visit_id") DO NOTHING
"""

SQL_INSERT_REJECT = """
INSERT INTO "RejectedRecords"
("visit_id","user_id","store_id","store_cat","city","timestamp","duration_s","reason","ingested_at")
VALUES (%s,%s,%s,%s,%s,%s,%s,%s, NOW()::timestamp)
ON CONFLICT ON CONSTRAINT rejectedrecords_uk DO NOTHING
"""

def try_insert_raw(cur, row) -> bool:
    """Devuelve True si se insertó (era nuevo). False si ya existía ese visit_id."""
    cur.execute(SQL_INSERT_RAW, row)
    return cur.rowcount == 1

def is_exact_duplicate(cur, row) -> bool:
    """Devuelve True si ya existe una fila idéntica (mismo visit_id y mismo resto de campos)."""
    cur.execute(SQL_IS_EXACT_DUP, row)
    return cur.fetchone() is not None

def insert_reject(cur, row, reason: str) -> None:
    cur.execute(SQL_INSERT_REJECT, (*row, reason))

def load_raw_from_csv(csv_path: str) -> None:
    df = pl.read_csv(csv_path, infer_schema=False)
    rows = df.select(RAW_COLS).rows()

    inserted = 0
    skipped_exact_dups = 0
    rejected = 0
    total = 0

    cfg = PgConfig.from_env() 
    with get_conn(cfg) as conn:
        with conn.cursor() as cur:
            for row in rows:
                total += 1

                if try_insert_raw(cur, row):
                    inserted += 1
                else:
                    if is_exact_duplicate(cur, row):
                        skipped_exact_dups += 1
                    else:
                        insert_reject(cur, row, "visit_id_conflict_different_payload")
                        rejected += 1

                # opcional: progreso cada 1000 filas
                if total % 1000 == 0:
                    print(f"[Load] processed={total} inserted={inserted} exact_dups={skipped_exact_dups} rejected={rejected}")

        conn.commit()

    print(f"[Load DONE] processed={total} inserted={inserted} exact_dups={skipped_exact_dups} rejected={rejected}")


if __name__ == "__main__":
    csv_path = os.getenv("CSV_PATH", "data/store_visits.csv")
    load_raw_from_csv(csv_path)
        