import polars as pl
from db import PgConfig, get_conn

SQL_INSERT_DIMUSER = """
INSERT INTO "DimUser" ("user_id","loaded_at")
VALUES (%s, NOW()::timestamp)
ON CONFLICT ("user_id") DO NOTHING
"""

SQL_INSERT_REJECT = """
INSERT INTO "RejectedRecords"
("visit_id","user_id","store_id","store_cat","city","timestamp","duration_s","reason","ingested_at")
VALUES (%s,%s,%s,%s,%s,%s,%s,%s, NOW()::timestamp)
ON CONFLICT ON CONSTRAINT rejectedrecords_uk DO NOTHING
"""

REJ_KEY_COLS = ["visit_id","user_id","store_id","store_cat","city","timestamp","duration_s","reason"]


def prep_rejected_rows(rejected_df: pl.DataFrame):
    df = rejected_df.with_columns([
        pl.col(c).cast(pl.Utf8).fill_null("").str.strip_chars().alias(c)
        for c in REJ_KEY_COLS
    ])
    return df.select(REJ_KEY_COLS).rows()


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

def split_dimuser_inputs(raw_df: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    df = raw_df.with_columns(
        pl.col("user_id").cast(pl.Utf8).str.strip_chars().alias("user_id_clean")
    )

    rejected_df = (
        df.filter(pl.col("user_id_clean").is_null() | (pl.col("user_id_clean") == ""))
          .with_columns(pl.lit("missing user_id").alias("reason"))
          .drop("user_id_clean")
    )

    dimuser_df = (
        df.filter(pl.col("user_id_clean").is_not_null() & (pl.col("user_id_clean") != ""))
          .select(pl.col("user_id_clean").alias("user_id"))
          .unique()
    )

    return dimuser_df, rejected_df

def load_dimuser(dimuser_df: pl.DataFrame, rejected_df: pl.DataFrame) -> None:
    users_rows = dimuser_df.select(["user_id"]).rows()
    rej_rows = prep_rejected_rows(rejected_df)

    cfg = PgConfig.from_env()
    with get_conn(cfg) as conn:
        with conn.cursor() as cur:
            cur.execute('SELECT COUNT(*) FROM "DimUser"')
            before_users = cur.fetchone()[0]
            cur.execute('SELECT COUNT(*) FROM "RejectedRecords"')
            before_rej = cur.fetchone()[0]

            cur.executemany(SQL_INSERT_DIMUSER, users_rows)
            cur.executemany(SQL_INSERT_REJECT, rej_rows)

        conn.commit()

        with conn.cursor() as cur:
            cur.execute('SELECT COUNT(*) FROM "DimUser"')
            after_users = cur.fetchone()[0]
            cur.execute('SELECT COUNT(*) FROM "RejectedRecords"')
            after_rej = cur.fetchone()[0]

    print(f"[Transform] DimUser inserted={after_users - before_users}")
    print(f"[Transform] Rejected inserted={after_rej - before_rej}")

    
if __name__ == "__main__":
    raw_df = fetch_rawstorevisits_df()
    dimuser_df, rejected_df = split_dimuser_inputs(raw_df)
    load_dimuser(dimuser_df, rejected_df)