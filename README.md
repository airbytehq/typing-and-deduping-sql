# typing-and-deduping-sql

This repo is a playground for experimenting with different ways of typing and deduping SQL queries on the path to 1-stream 1-table and replacing "normalization" and dbt.

Read more about this in the [PRD](https://docs.google.com/document/d/126SLzFLMS2QYXHAItx1cn03aj0HMuuDlajlUAVFtSIM/edit).

## Normalization Jobs Checklist

- [ ] Accepts batches of new data and types them safely while inserting the data into the final table
- [ ] Handles deduplication
- [ ] Handles per-row typing errors safely
- [ ] Handles composite primary keys
- [ ] Handles deduplication when multiple entries for a PK exist in the same batch
- [ ] Handles deduplication when multiple entries for a PK when inserted out-of-order across different batches

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
CREATE TABLE IF NOT EXISTS evan.users (
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
	DATE_TRUNC(_airbyte_extracted_at, MONTH)
) OPTIONS (
	description="users table"
)
;

CREATE TABLE IF NOT EXISTS evan.users_raw (
        `_airbyte_raw_id` STRING NOT NULL OPTIONS (description = 'Airbyte column, cannot be null')
    , `_airbyte_data` JSON NOT NULL OPTIONS (description = 'Airbyte column, cannot be null')
    , `_airbyte_extracted_at` TIMESTAMP NOT NULL OPTIONS (description = 'Airbyte column, cannot be null')
    , `_airbyte_loaded_at` TIMESTAMP
)
-- PARTITION BY (
-- 	DATE_TRUNC(_airbyte_extracted_at, MONTH)
-- )
;

TRUNCATE TABLE `evan`.`users_raw`;
TRUNCATE TABLE `evan`.`users`;

-- Load in the data (CSV)
-- NOTE: We have to use overwrite because BQ can handle writing CSVs to non-nullable columns.  This will alter the table to match the CSV and remove any partitioning or non-nullable-ness
-- NOTE: These all need to be within a transaction to gaunrentee previous statements are commited and ready before the next.  Thanks BQ!

BEGIN TRANSACTION;
	LOAD DATA OVERWRITE evan.users_raw
	FROM FILES (
	  format = 'CSV',
	  uris = ['gs://airbyte-performance-testing-public/typing-deduping-testing/users_raw.csv'],
	  skip_leading_rows = 1,
	  field_delimiter = "\t"
	);

	-- WAIT a ~15 seconds.  Just because bigquery says the query completed, doesn't mean it is ready yet. WTF.

	-- fix CSV load problems
	ALTER TABLE evan.users_raw ADD COLUMN _airbyte_data_json JSON;
	UPDATE evan.users_raw SET _airbyte_data_json = PARSE_JSON(_airbyte_data) WHERE _airbyte_data_json IS NULL;
	-- WAIT a ~15 seconds.  Just because bigquery says the query completed, doesn't mean it is ready yet. WTF.
	ALTER TABLE evan.users_raw DROP COLUMN _airbyte_data;
	ALTER TABLE evan.users_raw RENAME COLUMN _airbyte_data_json to _airbyte_data;

	-- update the _airbyte_raw_ids each time
	UPDATE evan.users_raw
	SET `_airbyte_raw_id` = GENERATE_UUID()
	WHERE `_airbyte_loaded_at` IS NULL
	;
COMMIT TRANSACTION;

select count(*) from evan.users_raw;
select count(*) from evan.users;

-- do it; loaded from .sql files in this repo
CALL evan._airbyte_type_dedupe();
```

### BigQuery Observations

- Loading 1M records from CSV hosted on Google Cloud took about ~10 seconds.
- Typing and Deduplicating 1M rows took ~11 seconds
