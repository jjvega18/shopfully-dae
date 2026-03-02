# ShopFully — Data Analytics Engineering Test

Pipeline to load raw store visits from CSV into Postgres and build a simple star schema:
- Raw: `RawStoreVisits` + `RejectedRecords`
- Dimensions: `DimUser`, `DimStore`
- Fact: `FactVisits`

## Repo structure
- `src/`: ETL (load + transforms + runner)
- `sql/`: DDL + test queries
- `data/`: input CSV

## Data model (tables)
Created by `sql/shopfully_ddl.sql`:
- `DimUser(user_sk, user_id, loaded_at)`
- `DimStore(store_sk, store_id, store_cat, store_city, loaded_at)`
- `FactVisits(visit_sk, visit_id, user_sk, store_sk, visit_ts, duration_s)`
- `RawStoreVisits(...)`
- `RejectedRecords(...)`

## Run with Docker
```bash
docker compose up -d --build
docker compose exec etl python src/Run.py
