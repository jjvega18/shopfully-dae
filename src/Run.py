# src/run_all.py
import os
import sys
import traceback

from Load import load_raw_from_csv
from Transform_DimUser import fetch_rawstorevisits_df, split_dimuser_inputs, load_dimuser
from Transform_DimStore import split_dimstore_inputs, load_dimstore
from Transform_FactVisits import run as run_factvisits


def main() -> int:
    try:
        csv_path = os.getenv("CSV_PATH", "data/store_visits.csv")

        print("\n[RunAll] 1/4 Load RawStoreVisits")
        load_raw_from_csv(csv_path)

        print("\n[RunAll] 2/4 Transform DimUser")
        raw_df = fetch_rawstorevisits_df()
        dimuser_df, rej_user_df = split_dimuser_inputs(raw_df)
        load_dimuser(dimuser_df, rej_user_df)

        print("\n[RunAll] 3/4 Transform DimStore")
        dimstore_df, rej_store_df = split_dimstore_inputs(raw_df)
        load_dimstore(dimstore_df, rej_store_df)

        print("\n[RunAll] 4/4 Transform FactVisits")
        run_factvisits()

        print("\n[RunAll] done")
        return 0

    except Exception:
        print("\n[RunAll] failed")
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    raise SystemExit(main())