import polars as pl
from db import PgConfig, get_conn

SQL_INSERT_REJECT = """
INSERT INTO "RejectedRecords"
("visit_id","user_id","store_id","store_cat","city","timestamp","duration_s","reason","ingested_at")
VALUES (%s,%s,%s,%s,%s,%s,%s,%s, NOW()::timestamp)
ON CONFLICT ON CONSTRAINT rejectedrecords_uk DO NOTHING
"""

SQL_UPSERT_DIMSTORE = """
INSERT INTO "DimStore" ("store_id","store_cat","store_city","loaded_at")
VALUES (%s,%s,%s, NOW()::timestamp)
ON CONFLICT ("store_id") DO UPDATE
SET
  "store_cat"  = EXCLUDED."store_cat",
  "store_city" = EXCLUDED."store_city"
WHERE "DimStore"."store_cat"  IS DISTINCT FROM EXCLUDED."store_cat"
   OR "DimStore"."store_city" IS DISTINCT FROM EXCLUDED."store_city"
"""

REJ_KEY_COLS = ["visit_id","user_id","store_id","store_cat","city","timestamp","duration_s","reason"]


def fetch_rawstorevisits_df() -> pl.DataFrame:
    cfg = PgConfig.from_env()
    sql = """
    SELECT visit_id, user_id, store_id, store_cat, city, "timestamp", duration_s, ingested_at
    FROM "RawStoreVisits"
    """
    with get_conn(cfg) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description]

    return pl.DataFrame(rows, schema=cols, orient="row")
def split_dimstore_inputs(raw_df: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    df = (
        raw_df
        .with_columns([
            pl.col("store_id").cast(pl.Utf8).str.strip_chars().alias("store_id_clean"),
            pl.col("store_cat").cast(pl.Utf8).str.strip_chars().alias("store_cat_tmp"),
            pl.col("city").cast(pl.Utf8).str.strip_chars().alias("store_city_tmp"),
        ])
        .with_columns([
            pl.when(pl.col("store_cat_tmp").is_null() | (pl.col("store_cat_tmp") == ""))
              .then(None)
              .otherwise(pl.col("store_cat_tmp").str.to_lowercase())
              .alias("store_cat_clean"),
            pl.when(pl.col("store_city_tmp").is_null() | (pl.col("store_city_tmp") == ""))
              .then(None)
              .otherwise(pl.col("store_city_tmp").str.to_titlecase())
              .alias("store_city_clean"),
        ])
        .drop(["store_cat_tmp", "store_city_tmp"])
    )

    # Reject only for missing store_id 
    rejected_df = (
        df.filter(pl.col("store_id_clean").is_null() | (pl.col("store_id_clean") == ""))
          .with_columns(pl.lit("missing_store_id").alias("reason"))
          .drop(["store_id_clean", "store_cat_clean", "store_city_clean"])
    )

    # Keep latest per store_id (use ingested_at)
    dimstore_df = (
        df.filter(pl.col("store_id_clean").is_not_null() & (pl.col("store_id_clean") != ""))
          .sort("ingested_at", descending=True)
          .unique(subset=["store_id_clean"], keep="first")
          .select([
              pl.col("store_id_clean").alias("store_id"),
              pl.col("store_cat_clean").alias("store_cat"),
              pl.col("store_city_clean").alias("store_city"),
          ])
    )

    return dimstore_df, rejected_df

def load_dimstore(dimstore_df: pl.DataFrame, rejected_df: pl.DataFrame) -> None:
    store_rows = dimstore_df.select(["store_id", "store_cat", "store_city"]).rows()
    rej_rows = rejected_df.select(
        ["visit_id","user_id","store_id","store_cat","city","timestamp","duration_s","reason"]
    ).rows()

    cfg = PgConfig.from_env()
    with get_conn(cfg) as conn:
        with conn.cursor() as cur:
            cur.execute('SELECT COUNT(*) FROM "DimStore"')
            before_store = cur.fetchone()[0]
            cur.execute('SELECT COUNT(*) FROM "RejectedRecords"')
            before_rej = cur.fetchone()[0]

            cur.executemany(SQL_UPSERT_DIMSTORE, store_rows)
            cur.executemany(SQL_INSERT_REJECT, rej_rows) 

        conn.commit()

        with conn.cursor() as cur:
            cur.execute('SELECT COUNT(*) FROM "DimStore"')
            after_store = cur.fetchone()[0]
            cur.execute('SELECT COUNT(*) FROM "RejectedRecords"')
            after_rej = cur.fetchone()[0]

    print(f"[Transform] DimStore inserted={after_store - before_store}")
    print(f"[Transform] Rejected inserted={after_rej - before_rej}")
#-----------------------------------------------------------------------------------------


# FactVisits
#-----------------------------------------------------------------------------------------
if __name__ == "__main__":
    raw_df = fetch_rawstorevisits_df()
    dimstore_df, rej_store_df = split_dimstore_inputs(raw_df)
    load_dimstore(dimstore_df, rej_store_df)