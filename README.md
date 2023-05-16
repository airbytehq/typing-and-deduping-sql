# typing-and-deduping-sql-experiments

This repo is a playground for experimenting with different ways of typing and deduping SQL queries on the path to 1-stream 1-table and replacing "normalization" and dbt.

Read more about this in the [PRD](https://docs.google.com/document/d/126SLzFLMS2QYXHAItx1cn03aj0HMuuDlajlUAVFtSIM/edit).

## Snowflake Testing Notes.

If you want to seed a big test database:

```sql
-- Create the table
CREATE SCHEMA IF NOT EXISTS Z_AIRBYTE;
CREATE TABLE IF NOT EXISTS Z_AIRBYTE.USERS_RAW (
    "_airbyte_data" variant NOT NULL, -- Airbyte column, cannot be null
    "_airbyte_raw_id" VARCHAR(36) NOT NULL, -- Airbyte column, cannot be null
    "_airbyte_read_at" timestamp NOT NULL, -- Airbyte column, cannot be null
    "_airbyte_typed_at" timestamp -- Airbyte column
);
TRUNCATE TABLE Z_AIRBYTE.USERS_RAW;

-- Load in the data
COPY INTO Z_AIRBYTE.USERS_RAW
FROM 'gcs://airbyte-performance-testing-public/typing-deduping-testing/users_raw.csv'
FILE_FORMAT = (TYPE = 'CSV')
;
```

### Snowflake Observations

- Handling nested-JSON in CSV didn't really work for stringified JSON. Trying JSON files...
- It takes about 45 seconds to "spin up a thread" to start the `COPY INTO` statement. If the file was 10 lines or 1M lines, the startup cost was more-or-less the same.
