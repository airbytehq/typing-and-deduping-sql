# typing-and-deduping-sql

This repo is a playground for experimenting with different ways of typing and deduping SQL queries on the path to 1-stream 1-table and replacing "normalization" and dbt.

Read more about this projedt in https://github.com/airbytehq/airbyte/issues/26028 and the [PRD](https://docs.google.com/document/d/126SLzFLMS2QYXHAItx1cn03aj0HMuuDlajlUAVFtSIM/edit).

## Normalization Jobs Checklist

- [x] Accepts batches of new data and types them safely while inserting the data into the final table
- [x] Handles deduplication
- [x] Handles per-row typing errors safely
- [ ] Handles composite primary keys
- [x] Handles deduplication when multiple entries for a PK exist in the same batch
- [x] Handles deduplication when multiple entries for a PK when inserted out-of-order across different batches
- [x] Handles CDC deletes and releated shenanigans. 

## Snowflake Testing Notes.

If you want to seed a big test database:

```sql
-- Create the tables
CREATE TABLE PUBLIC.USERS (
    "id" int PRIMARY KEY, -- PK cannot be null, but after raw insert and before typing, row will be null
    "first_name" text,
    "age" int,
    "address" variant,
    "updated_at" timestamp NOT NULL,
    "_airbyte_meta" variant NOT NULL, -- Airbyte column, cannot be null
    "_airbyte_raw_id" VARCHAR(36) NOT NULL, -- Airbyte column, cannot be null
    "_airbyte_extracted_at" timestamp NOT NULL -- Airbyte column, cannot be null
);
CREATE SCHEMA IF NOT EXISTS Z_AIRBYTE;
CREATE TABLE IF NOT EXISTS Z_AIRBYTE.USERS_RAW (
    "_airbyte_raw_id" VARCHAR(36) NOT NULL PRIMARY KEY, -- Airbyte column, cannot be null
    "_airbyte_data" variant NOT NULL, -- Airbyte column, cannot be null
    "_airbyte_extracted_at" timestamp NOT NULL, -- Airbyte column, cannot be null
    "_airbyte_loaded_at" timestamp -- Airbyte column
);
TRUNCATE TABLE PUBLIC.USERS;
TRUNCATE TABLE Z_AIRBYTE.USERS_RAW;

-- Load in the data (CSV)
COPY INTO Z_AIRBYTE.USERS_RAW
FROM 'gcs://airbyte-performance-testing-public/typing-deduping-testing/users_raw.csv'
FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = '\t' SKIP_HEADER = 1 ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE)
FORCE = TRUE
PURGE = FALSE
;
-- update the _airbyte_raw_ids each time
UPDATE Z_AIRBYTE.USERS_RAW
SET "_airbyte_raw_id" = UUID_STRING()
WHERE "_airbyte_loaded_at" IS NULL
;
```

Loading 1M records from CSV hosted on Google Cloud took about ~75 seconds.

And if you want to insert the data again, to emulate another sync with 1M records:

```sql
-- Load in the data (CSV) w/o truncating
COPY INTO Z_AIRBYTE.USERS_RAW
FROM 'gcs://airbyte-performance-testing-public/typing-deduping-testing/users_raw.csv'
FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = '\t' SKIP_HEADER = 1 ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE)
FORCE = TRUE
PURGE = FALSE
;
-- update the _airbyte_raw_ids each time
UPDATE Z_AIRBYTE.USERS_RAW
SET "_airbyte_raw_id" = UUID_STRING()
WHERE "_airbyte_loaded_at" IS NULL
;
```

To Dump the file from postgres (where it was generated locally):

```sql
COPY z_airbyte.users_raw to '/Users/evan/workspace/airbyte/typing-and-deduping-sql-experiments/data/users_raw.csv'
WITH (FORMAT CSV, HEADER TRUE, ENCODING 'UTF8', quote '|', delimiter E'\t');
```

There's also a `gcs://airbyte-performance-testing-public/typing-deduping-testing/users_raw_errors.csv` file that has 1M records with 10 rows having a type error. This is useful for testing the error handling.

### Snowflake Observations

- Can't use JSON files to load, 16MB limit for the whole file. Can't use normal CSV, because double-quoting JSON confuses snowflake's parser. Solution was to use ';' as `FIELD_DELIMITER`
- CSV header order is meaningless. The header order and table order MUST match.
- It takes about 45 seconds to "spin up a thread" to start the `COPY INTO` statement. If the file was 10 lines or 1M lines, the startup cost was more-or-less the same.

## Bigquery Testing Notes

If you want to seed a big test database:

```sql
-- Load in the data (CSV)
-- NOTE: We have to use overwrite because BQ can handle writing CSVs to non-nullable columns.  This will alter the table to match the CSV and remove any partitioning or non-nullable-ness

CREATE TABLE IF NOT EXISTS testing_evan_2052.users (
    `id` INT64 OPTIONS (description = 'PK cannot be null, but after raw insert and before typing, row will be temporarily null')
  , `first_name` STRING
  , `age` INT64
  , `address` JSON
  , `updated_at` TIMESTAMP
  , `_airbyte_meta` JSON NOT NULL OPTIONS (description = 'Airbyte column, cannot be null')
  , `_airbyte_raw_id` STRING NOT NULL OPTIONS (description = 'Airbyte column, cannot be null')
  , `_airbyte_extracted_at` TIMESTAMP NOT NULL OPTIONS (description = 'Airbyte column, cannot be null')
)
PARTITION BY (
	DATE_TRUNC(_airbyte_extracted_at, DAY)
	-- TODO: Learn about partition_expiration_days https://cloud.google.com/bigquery/docs/creating-partitioned-tables
) CLUSTER BY
  id, _airbyte_extracted_at
OPTIONS (
	description="users table"
)
;

CREATE TABLE IF NOT EXISTS testing_evan_2052.users_raw (
        `_airbyte_raw_id` STRING NOT NULL OPTIONS (description = 'Airbyte column, cannot be null')
    , `_airbyte_data` JSON NOT NULL OPTIONS (description = 'Airbyte column, cannot be null')
    , `_airbyte_extracted_at` TIMESTAMP NOT NULL OPTIONS (description = 'Airbyte column, cannot be null')
    , `_airbyte_loaded_at` TIMESTAMP
)
-- no partition, no cluster
;

TRUNCATE TABLE `testing_evan_2052`.`users_raw`;
TRUNCATE TABLE `testing_evan_2052`.`users`;

-- Load in the data (CSV)
-- NOTE: We have to use overwrite because BQ can handle writing CSVs to non-nullable columns.  This will alter the table to match the CSV and remove any partitioning or non-nullable-ness

DROP TABLE IF EXISTS testing_evan_2052.users_raw_tmp;
LOAD DATA OVERWRITE testing_evan_2052.users_raw_tmp
FROM FILES (
  format = 'CSV',
  uris = ['gs://airbyte-performance-testing-public/typing-deduping-testing/users_raw.csv'],
  skip_leading_rows = 1,
  field_delimiter = "\t"
);

INSERT INTO testing_evan_2052.users_raw (
  _airbyte_raw_id, _airbyte_data, _airbyte_extracted_at
) SELECT
    _airbyte_raw_id
  , PARSE_JSON(_airbyte_data)
  , CAST(_airbyte_extracted_at AS timestamp)
FROM testing_evan_2052.users_raw_tmp;

DROP TABLE IF EXISTS testing_evan_2052.users_raw_tmp;

-- update the _airbyte_raw_ids each time
UPDATE testing_evan_2052.users_raw
SET `_airbyte_raw_id` = GENERATE_UUID()
WHERE `_airbyte_loaded_at` IS NULL
;

select count(*) from testing_evan_2052.users_raw;
select count(*) from testing_evan_2052.users;

-- do it; loaded from .sql files in this repo
CALL testing_evan_2052._airbyte_type_dedupe();
```

### BigQuery Observations

- Loading 1M records from CSV hosted on Google Cloud took about ~10 seconds.
- Typing and Deduplicating 1M rows took ~11 seconds
