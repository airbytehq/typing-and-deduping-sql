# typing-and-deduping-sql-experiments

This repo is a playground for experimenting with different ways of typing and deduping SQL queries on the path to 1-stream 1-table and replacing "normalization" and dbt.

Read more about this in the [PRD](https://docs.google.com/document/d/126SLzFLMS2QYXHAItx1cn03aj0HMuuDlajlUAVFtSIM/edit).

## Snowflake Testing Notes.

If you want to seed a big test database:

```sql
-- Create the table
CREATE SCHEMA IF NOT EXISTS Z_AIRBYTE;
CREATE TABLE IF NOT EXISTS Z_AIRBYTE.USERS_RAW (
    "_airbyte_raw_id" VARCHAR(36) NOT NULL, -- Airbyte column, cannot be null
    "_airbyte_data" variant NOT NULL, -- Airbyte column, cannot be null
    "_airbyte_read_at" timestamp NOT NULL, -- Airbyte column, cannot be null
    "_airbyte_typed_at" timestamp -- Airbyte column
);
TRUNCATE TABLE Z_AIRBYTE.USERS_RAW;

-- Load in the data (CSV)
COPY INTO Z_AIRBYTE.USERS_RAW
FROM 'gcs://airbyte-performance-testing-public/typing-deduping-testing/users_raw.csv'
FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = '\t' SKIP_HEADER = 1 ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE)
FORCE = TRUE
PURGE = FALSE
;
```

To Dump the file from postgres:

```sql
COPY z_airbyte.users_raw to '/Users/evan/workspace/airbyte/typing-and-deduping-sql-experiments/data/users_raw.csv'
WITH (FORMAT CSV, HEADER TRUE, ENCODING 'UTF8', quote '|', delimiter E'\t');
```

### Snowflake Observations

- Can't use JSON files to load, 16MB limit for the whole file. Can't use normal CSV, because double-quoting JSON confuses snowflake's parser. Solution was to use ';' as `FIELD_DELIMITER`
- CSV header order is meaningless. The header order and table order MUST match.
- It takes about 45 seconds to "spin up a thread" to start the `COPY INTO` statement. If the file was 10 lines or 1M lines, the startup cost was more-or-less the same.
