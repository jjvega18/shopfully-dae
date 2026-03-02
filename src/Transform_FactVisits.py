from __future__ import annotations
from datetime import datetime
import polars as pl
from db import PgConfig, get_conn


SQL_RAW_FACT_INCREMENTAL = """
SELECT
  r.visit_id, r.user_id, r.store_id, r.store_cat, r.city, r."timestamp", r.duration_s, r.ingested_at
FROM "RawStoreVisits" r
WHERE NOT EXISTS (
  SELECT 1 FROM "FactVisits" f WHERE f.visit_id = r.visit_id
)
"""

SQL_DIMUSER_MAP = """SELECT user_id, user_sk FROM "DimUser" """
SQL_DIMSTORE_MAP = """SELECT store_id, store_sk FROM "DimStore" """

SQL_INSERT_REJECT = """
INSERT INTO "RejectedRecords"
("visit_id","user_id","store_id","store_cat","city","timestamp","duration_s","reason","ingested_at")
VALUES (%s,%s,%s,%s,%s,%s,%s,%s, NOW()::timestamp)
ON CONFLICT ON CONSTRAINT rejectedrecords_uk DO NOTHING
"""

SQL_INSERT_FACT = """
INSERT INTO "FactVisits" ("visit_id","user_sk","store_sk","visit_ts","duration_s")
VALUES (%s,%s,%s,%s,%s)
ON CONFLICT ("visit_id") DO NOTHING
"""

REJ_COLS = ["visit_id", "user_id", "store_id", "store_cat", "city", "timestamp", "duration_s", "reason"]
FACT_COLS = ["visit_id", "user_sk", "store_sk", "visit_ts", "duration_s"]


#RAW DATA
def fetch_df(sql: str) -> pl.DataFrame:
    cfg = PgConfig.from_env()
    with get_conn(cfg) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description]
    return pl.DataFrame(rows, schema=cols, orient="row")


def count_rows(table: str) -> int:
    cfg = PgConfig.from_env()
    with get_conn(cfg) as conn:
        with conn.cursor() as cur:
            cur.execute(f'SELECT COUNT(*) FROM "{table}"')
            return int(cur.fetchone()[0])

def fetch_raw_fact_incremental() -> pl.DataFrame:
    return fetch_df(SQL_RAW_FACT_INCREMENTAL)


def fetch_dim_maps() -> tuple[pl.DataFrame, pl.DataFrame]:
    return fetch_df(SQL_DIMUSER_MAP), fetch_df(SQL_DIMSTORE_MAP)


#VALIDATION + PREP
def prepare_fact(raw_df: pl.DataFrame, dim_user: pl.DataFrame, dim_store: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame, dict]:

    df = raw_df.with_columns([
        pl.col("visit_id").cast(pl.Utf8).str.strip_chars().alias("visit_id_clean"),
        pl.col("user_id").cast(pl.Utf8).str.strip_chars().alias("user_id_clean"),
        pl.col("store_id").cast(pl.Utf8).str.strip_chars().alias("store_id_clean"),
        pl.col("timestamp").cast(pl.Utf8).str.strip_chars().alias("timestamp_clean"),
        pl.col("duration_s").cast(pl.Utf8).str.strip_chars().alias("duration_s_clean"),
    ])

    #VALIDATION VISIT_TS
    now_dt = pl.lit(datetime.now())
    df = df.with_columns([
        pl.col("timestamp_clean")
          .str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S", strict=False)
          .alias("visit_ts_1"),  
        pl.col("timestamp_clean")
          .str.strptime(pl.Datetime, "%d/%m/%Y %H:%M", strict=False)
          .alias("visit_ts_2"),  
    ]).with_columns(
        pl.coalesce([pl.col("visit_ts_1"), pl.col("visit_ts_2")]).alias("visit_ts")  
    ).drop(["visit_ts_1", "visit_ts_2"]).with_columns(  
        (pl.col("visit_ts").is_not_null() & (pl.col("visit_ts") <= now_dt)).alias("visit_ts_ok")
    )

    #VALIDATION DURATION_S
    df = df.with_columns([
        pl.when(pl.col("duration_s_clean").is_null() | (pl.col("duration_s_clean") == ""))
          .then(None)
          .otherwise(pl.col("duration_s_clean").cast(pl.Int64, strict=False))
          .alias("duration_s_int"),

        pl.when(pl.col("duration_s_clean").is_null() | (pl.col("duration_s_clean") == ""))
          .then(False)
          .otherwise(pl.col("duration_s_clean").cast(pl.Int64, strict=False).is_null())
          .alias("duration_not_castable"),
    ]).with_columns(
        (
            ~pl.col("duration_not_castable")
            & (pl.col("duration_s_int").is_null() | (pl.col("duration_s_int") >= 0))
        ).alias("duration_ok")
    )

    # LOOKUPS
    du = dim_user.select([
        pl.col("user_id").cast(pl.Utf8).str.strip_chars().alias("user_id_clean"),
        pl.col("user_sk"),
    ])

    ds = dim_store.select([
        pl.col("store_id").cast(pl.Utf8).str.strip_chars().alias("store_id_clean"),
        pl.col("store_sk"),
    ])

    df = (
        df.join(du, on="user_id_clean", how="left")
        .join(ds, on="store_id_clean", how="left")
    )

    # VALIDATION + REASONING OF REJECTIONS:
    df = df.with_columns(
        pl.when(pl.col("visit_id_clean").is_null() | (pl.col("visit_id_clean") == ""))
          .then(pl.lit("missing visit_id"))
        .when(pl.col("user_sk").is_null())
          .then(pl.lit("missing/unknown user"))
        .when(pl.col("store_sk").is_null())
          .then(pl.lit("missing/unknown store"))
        .when(~pl.col("visit_ts_ok"))
          .then(pl.lit("invalid visit_ts"))
        .when(~pl.col("duration_ok"))
          .then(pl.lit("invalid duration_s"))
        .otherwise(None)
        .alias("reason")
    )


    rejected_df = df.filter(pl.col("reason").is_not_null()).select(REJ_COLS)

    fact_df = df.filter(pl.col("reason").is_null()).select([
        pl.col("visit_id_clean").alias("visit_id"),
        pl.col("user_sk"),
        pl.col("store_sk"),
        pl.col("visit_ts"),
        pl.col("duration_s_int").alias("duration_s"),
    ])

    metrics = {
        "soft_duration_le_1": int(
            fact_df.filter(pl.col("duration_s").is_not_null() & (pl.col("duration_s") <= 1)).height
        )
    }
    return fact_df, rejected_df, metrics


# -----------------------------------------------------------------------------
# 3) Loads
# -----------------------------------------------------------------------------
def load_rejected(rejected_df: pl.DataFrame) -> int:
    if rejected_df.is_empty():
        return 0
    
    rejected_df = rejected_df.with_columns([
        pl.col(c).cast(pl.Utf8).fill_null("").str.strip_chars().alias(c)
        for c in REJ_COLS
    ])
    rows = rejected_df.select(REJ_COLS).rows()

    before = count_rows("RejectedRecords")

    cfg = PgConfig.from_env()
    with get_conn(cfg) as conn:
        with conn.cursor() as cur:
            cur.executemany(SQL_INSERT_REJECT, rows)
        conn.commit()

    after = count_rows("RejectedRecords")
    return after - before


def load_fact(fact_df: pl.DataFrame) -> int:
    if fact_df.is_empty():
        return 0

    rows = fact_df.select(FACT_COLS).rows()

    before = count_rows("FactVisits")

    cfg = PgConfig.from_env()
    with get_conn(cfg) as conn:
        with conn.cursor() as cur:
            cur.executemany(SQL_INSERT_FACT, rows)
        conn.commit()

    after = count_rows("FactVisits")
    return after - before


def run() -> None:
    raw_inc = fetch_raw_fact_incremental()
    dim_user, dim_store = fetch_dim_maps()

    fact_df, rej_df, metrics = prepare_fact(raw_inc, dim_user, dim_store)

    rej_ins = load_rejected(rej_df)
    fact_ins = load_fact(fact_df)

    print(f"[Transform] FactVisits inserted={fact_ins}")
    print(f"[Transform] Rejected inserted={rej_ins}")
    print(f"[Transform] metrics soft_duration_le_1={metrics['soft_duration_le_1']}")


if __name__ == "__main__":
    run()