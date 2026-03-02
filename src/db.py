import os
from dataclasses import dataclass
import psycopg

@dataclass
class PgConfig:
    host: str
    port: str
    dbname: str
    user: str
    password: str

    @classmethod
    def from_env(cls) -> "PgConfig":
        return cls(
            host=os.environ["PG_HOST"],
            port=os.environ["PG_PORT"],
            dbname=os.environ["PG_DB"],
            user=os.environ["PG_USER"],
            password=os.environ["PG_PASSWORD"],
        )

def get_conn(cfg: PgConfig) -> psycopg.Connection:
    return psycopg.connect(
        host=cfg.host,
        port=cfg.port,
        dbname=cfg.dbname,
        user=cfg.user,
        password=cfg.password,
    )