CREATE TABLE "DimUser" (
  "user_sk"   bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  "user_id"   text UNIQUE NOT NULL,
  "loaded_at" timestamp NOT NULL
);

CREATE TABLE "DimStore" (
  "store_sk"   bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  "store_id"   text UNIQUE NOT NULL,
  "store_cat"  text,
  "store_city" text,
  "loaded_at"  timestamp NOT NULL
);

CREATE TABLE "FactVisits" (
  "visit_sk"   bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  "visit_id"   text UNIQUE NOT NULL,
  "user_sk"    bigint NOT NULL REFERENCES "DimUser" ("user_sk"),
  "store_sk"   bigint NOT NULL REFERENCES "DimStore" ("store_sk"),
  "visit_ts"   timestamp NOT NULL,
  "duration_s" integer
);

CREATE TABLE IF NOT EXISTS "RawStoreVisits" (
  "visit_id"   text UNIQUE,
  "user_id"    text,
  "store_id"   text,
  "store_cat"  text,
  "city"       text,
  "timestamp"  text,
  "duration_s" text,
  "ingested_at" timestamp NOT NULL
);

CREATE TABLE "RejectedRecords" (
  "rejected_id"  bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  "ingested_at"  timestamp NOT NULL,
  "reason"       text NOT NULL,
  "visit_id"     text,
  "user_id"      text,
  "store_id"     text,
  "store_cat"    text,
  "city"         text,
  "timestamp"    text,
  "duration_s"   text,
  CONSTRAINT rejectedrecords_uk UNIQUE
  ("visit_id","user_id","store_id","store_cat","city","timestamp","duration_s","reason")
);