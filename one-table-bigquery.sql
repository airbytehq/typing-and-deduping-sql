/*
SQL Experiments for Typing and Normalizing AirbyteRecords in 1 table
Run me on BQ

Schema:
{
  "id": "number",
  "first_name": ["string", null],
  "age": ["number", null],
  "address": [null, {
    "street": "string",
    "zip": "string"
  }],
  "updated_at": timestamp
}

KNOWN LIMITATIONS
* Only one error type shown per row, the first one sequentially
* There's a full table scan used for de-duplication.  This can be made more efficient...
* It would be better to show the actual error message from the DB, not custom "this column is bad" strings

*/

-- Set up the Experiment
-- Assumption: We can build the table at the start of the sync based only on the schema we get from the source/configured catalog

DROP TABLE IF EXISTS testing_evan_2052.users;
DROP TABLE IF EXISTS testing_evan_2052.users_raw;

CREATE TABLE testing_evan_2052.users (
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

-------------------------------------
--------- TYPE AND DEDUPE -----------
-------------------------------------

CREATE OR REPLACE PROCEDURE testing_evan_2052._airbyte_prepare_raw_table()
BEGIN
  CREATE TABLE IF NOT EXISTS testing_evan_2052.users_raw (
        `_airbyte_raw_id` STRING NOT NULL OPTIONS (description = 'Airbyte column, cannot be null')
      , `_airbyte_data` JSON NOT NULL OPTIONS (description = 'Airbyte column, cannot be null')
      , `_airbyte_extracted_at` TIMESTAMP NOT NULL OPTIONS (description = 'Airbyte column, cannot be null')
      , `_airbyte_loaded_at` TIMESTAMP
  )
  PARTITION BY (
    DATE_TRUNC(_airbyte_extracted_at, DAY)
  ) CLUSTER BY
    _airbyte_loaded_at
  ;
END
;

CREATE OR REPLACE PROCEDURE testing_evan_2052._airbyte_type_dedupe()
OPTIONS (strict_mode=FALSE)
BEGIN
  DECLARE missing_pk_count INT64;

  BEGIN TRANSACTION;

    -- Step 1: Validate the incoming data
    -- We can't really do this properly in the pure-SQL example here, but we should throw if any row doesn't have a PK

    SET missing_pk_count = (
      SELECT COUNT(1)
      FROM testing_evan_2052.users_raw
      WHERE
        `_airbyte_loaded_at` IS NULL
        AND SAFE_CAST(JSON_VALUE(`_airbyte_data`, '$.id') as INT64) IS NULL
      );

    IF missing_pk_count > 0 THEN
      RAISE USING message = FORMAT("Raw table has %s rows missing a primary key", CAST(missing_pk_count AS STRING));
    END IF;

    -- Moving the data and deduping happens in a transaction to prevent duplicates from appearing
    -- BEGIN

    -- Step 2: Move the new data to the typed table
    INSERT INTO testing_evan_2052.users
    (
      id,
      first_name,
      age,
      updated_at,
      address,
      _airbyte_meta,
      _airbyte_raw_id,
      _airbyte_extracted_at
    )

    WITH intermediate_data AS (
      SELECT
        SAFE_CAST(JSON_VALUE(`_airbyte_data`, '$.id') as INT64) as id,
        SAFE_CAST(JSON_VALUE(`_airbyte_data`, '$.first_name') as STRING) as first_name,
        SAFE_CAST(JSON_VALUE(`_airbyte_data`, '$.age') as INT64) as age,
        CASE
          -- If the json value is not an object, return null. This also includes {"address": null}. Maybe we should return JSON'null' in that case?
          WHEN JSON_QUERY(`_airbyte_data`, '$.address') IS NULL
            OR JSON_TYPE(JSON_QUERY(`_airbyte_data`, '$.address')) != 'object'
            THEN NULL
          ELSE JSON_QUERY(`_airbyte_data`, '$.address')
        END as `address`, -- NOTE: For record properties remaining as JSON, you `JSON_QUERY`, not `JSON_VALUE`
        SAFE_CAST(JSON_VALUE(`_airbyte_data`, '$.updated_at') as TIMESTAMP) as updated_at,
        array_concat(
          CASE
            -- JSON_VALUE(JSON'{"id": {...}}', '$.id') returns NULL.
            -- so we use JSON_QUERY instead to check whether there should be a value here.
            -- If we used json_value, then we would falsely believe that id is unset, and therefore would not populate an error into airbyte_meta.
            WHEN (JSON_QUERY(`_airbyte_data`, '$.id') IS NOT NULL)
              AND (JSON_TYPE(JSON_QUERY(`_airbyte_data`, '$.id') != 'null'))
              -- But we do need JSON_VALUE here to get a SQL value rather than JSON.
              AND (SAFE_CAST(JSON_VALUE(`_airbyte_data`, '$.id') as INT64) IS NULL)
              THEN ["Problem with `id`"]
            ELSE []
          END,
          CASE
            WHEN (JSON_QUERY(`_airbyte_data`, '$.first_name') IS NOT NULL)
              AND (JSON_TYPE(JSON_QUERY(`_airbyte_data`, '$.first_name') != 'null'))
              AND (SAFE_CAST(JSON_VALUE(`_airbyte_data`, '$.first_name') as STRING) IS NULL)
              THEN ["Problem with `first_name`"]
            ELSE []
          END,
          CASE
            WHEN (JSON_QUERY(`_airbyte_data`, '$.age') IS NOT NULL)
              AND (JSON_TYPE(JSON_QUERY(`_airbyte_data`, '$.age') != 'null'))
              AND (SAFE_CAST(JSON_VALUE(`_airbyte_data`, '$.age') as INT64) IS NULL)
              THEN ["Problem with `age`"]
            ELSE []
          END,
          CASE
            WHEN (JSON_QUERY(`_airbyte_data`, '$.updated_at') IS NOT NULL)
              AND (JSON_TYPE(JSON_QUERY(`_airbyte_data`, '$.updated_at') != 'null'))
              AND (SAFE_CAST(JSON_VALUE(`_airbyte_data`, '$.updated_at') as TIMESTAMP) IS NULL)
              THEN ["Problem with `updated_at`"]
            ELSE []
          END,
          -- The nested CASE statement is identical to the one above that extracts `address` from `_airbyte_data`
          -- This means we're doing repeated work, e.g. checking the json type.
          -- We could probably do this a bit more smartly, but this format is easier to codegen. (and maybe bigquery is smart enough to optimize it anyway?)
          -- E.g. this is equivalent:
          -- CASE
          --   WHEN (JSON_QUERY(`_airbyte_data`, '$.address') IS NOT NULL)
          --     AND (JSON_TYPE(JSON_QUERY(`_airbyte_data`, '$.address')) NOT IN ('null', 'object'))
          --     THEN ["Problem with `address`"]
          --   ELSE []
          -- END,
          CASE
            WHEN (JSON_VALUE(`_airbyte_data`, '$.address') IS NOT NULL)
              AND (JSON_TYPE(JSON_VALUE(`_airbyte_data`, '$.address') != 'null'))
              AND (CASE
                  WHEN JSON_QUERY(`_airbyte_data`, '$.address') IS NULL
                    OR JSON_TYPE(JSON_QUERY(`_airbyte_data`, '$.address')) != 'object'
                    THEN NULL
                  ELSE JSON_QUERY(`_airbyte_data`, '$.address')
                END
                IS NULL)
              THEN ["Problem with `address`"]
            ELSE []
          END
        ) _airbyte_cast_errors,
        _airbyte_raw_id,
        _airbyte_extracted_at
      FROM testing_evan_2052.users_raw
      WHERE
        _airbyte_loaded_at IS NULL -- inserting only new/null values, we can recover from failed previous checkpoints
        AND JSON_EXTRACT(`_airbyte_data`, '$._ab_cdc_deleted_at') IS NULL -- Skip CDC deleted rows (old records are already cleared away above
    )

    SELECT
      `id`,
      `first_name`,
      `age`,
      `updated_at`,
      `address`,
      CASE
        WHEN array_length(_airbyte_cast_errors) = 0 THEN JSON'{"errors": []}'
        ELSE to_json(struct(_airbyte_cast_errors AS errors))
      END AS _airbyte_meta,
      _airbyte_raw_id,
      _airbyte_extracted_at
    FROM intermediate_data
    ;

    -- Step 3: Dedupe and clean the typed table
    -- This is a full table scan, but we need to do it this way to merge the new rows with the old to:
    --   * Consider the case in which there are multiple entries for the same PK in the new insert batch
    --	 * Consider the case in which the data in the new batch is older than the data in the typed table, and we only want to keep the newer (pre-existing) data
    --   * Order by the source's provided cursor and _airbyte_extracted_at to break any ties

    -- NOTE: Bigquery does not support using CTEs for delete statements: https://stackoverflow.com/questions/48130324/bigquery-delete-statement-to-remove-duplicates

    DELETE FROM testing_evan_2052.users
    WHERE
      -- Delete any rows which are not the most recent for a given PK
      `_airbyte_raw_id` IN (
        SELECT `_airbyte_raw_id` FROM (
          SELECT `_airbyte_raw_id`, row_number() OVER (
            PARTITION BY `id` ORDER BY `updated_at` DESC, `_airbyte_extracted_at` DESC
          ) as row_number FROM testing_evan_2052.users
        )
        WHERE row_number != 1
      )
      OR
      -- Delete rows that have been CDC deleted
      `id` IN (
        SELECT
          SAFE_CAST(JSON_VALUE(`_airbyte_data`, '$.id') as INT64) as id -- based on the PK which we know from the connector catalog
        FROM testing_evan_2052.users_raw
        WHERE JSON_VALUE(`_airbyte_data`, '$._ab_cdc_deleted_at') IS NOT NULL
      )
    ;

    -- Step 4: Remove old entries from Raw table
    DELETE FROM
      testing_evan_2052.users_raw
    WHERE
      `_airbyte_raw_id` NOT IN (
        SELECT `_airbyte_raw_id` FROM testing_evan_2052.users
      )
      AND
      JSON_VALUE(`_airbyte_data`, '$._ab_cdc_deleted_at') IS NULL -- we want to keep the final _ab_cdc_deleted_at=true entry in the raw table for the deleted record
    ;

    -- Step 5: Apply typed_at timestamp where needed
    UPDATE testing_evan_2052.users_raw
    SET `_airbyte_loaded_at` = CURRENT_TIMESTAMP()
    WHERE `_airbyte_loaded_at` IS NULL
    ;

  COMMIT TRANSACTION;
END;

----------------------------
--------- SYNC 1 -----------
----------------------------

CALL testing_evan_2052._airbyte_prepare_raw_table();

-- Load the raw data

INSERT INTO testing_evan_2052.users_raw (`_airbyte_data`, `_airbyte_raw_id`, `_airbyte_extracted_at`) VALUES (JSON'{   "id": 1,    "updated_at": "2020-01-01T00:00:00Z",   "first_name": "Evan",   "age": 38,   "address": {     "city": "San Francisco",     "zip": "94001"   } }', GENERATE_UUID(), CURRENT_TIMESTAMP());
INSERT INTO testing_evan_2052.users_raw (`_airbyte_data`, `_airbyte_raw_id`, `_airbyte_extracted_at`) VALUES (JSON'{   "id": 2,    "updated_at": "2020-01-01T00:00:01Z",   "first_name": "Brian",   "age": 39,   "address": {     "city": "Menlo Park",     "zip": "94002"   } }', GENERATE_UUID(), CURRENT_TIMESTAMP());
INSERT INTO testing_evan_2052.users_raw (`_airbyte_data`, `_airbyte_raw_id`, `_airbyte_extracted_at`) VALUES (JSON'{   "id": 3,    "updated_at": "2020-01-01T00:00:02Z",   "first_name": "Edward",   "age": 40,   "address": {     "city": "Sunyvale",     "zip": "94003"   } }', GENERATE_UUID(), CURRENT_TIMESTAMP());
INSERT INTO testing_evan_2052.users_raw (`_airbyte_data`, `_airbyte_raw_id`, `_airbyte_extracted_at`) VALUES (JSON'{   "id": 4,    "updated_at": "2020-01-01T00:00:03Z",   "first_name": "Joe",   "address": {     "city": "Seattle",     "zip": "98999"   } }', GENERATE_UUID(), CURRENT_TIMESTAMP());


CALL testing_evan_2052._airbyte_type_dedupe();

----------------------------
--------- SYNC 2 -----------
----------------------------

CALL testing_evan_2052._airbyte_prepare_raw_table();

-- Load the raw data
-- Age update for testing_evan_2052 (user 1)
-- There is an update for Brian (user 2, new address.zip)
-- There is an update for Edward (user 3, age is invalid)
-- No update for Joe (user 4)

INSERT INTO testing_evan_2052.users_raw (`_airbyte_data`, `_airbyte_raw_id`, `_airbyte_extracted_at`) VALUES (JSON'{   "id": 1,    "updated_at": "2020-01-02T00:00:00Z",   "first_name": "Evan",   "age": 39,   "address": {     "city": "San Francisco",     "zip": "94001"   } }', GENERATE_UUID(), CURRENT_TIMESTAMP());
INSERT INTO testing_evan_2052.users_raw (`_airbyte_data`, `_airbyte_raw_id`, `_airbyte_extracted_at`) VALUES (JSON'{   "id": 2,    "updated_at": "2020-01-02T00:00:01Z",   "first_name": "Brian",   "age": 39,   "address": {     "city": "Menlo Park",     "zip": "99999"   } }', GENERATE_UUID(), CURRENT_TIMESTAMP());
INSERT INTO testing_evan_2052.users_raw (`_airbyte_data`, `_airbyte_raw_id`, `_airbyte_extracted_at`) VALUES (JSON'{   "id": 3,    "updated_at": "2020-01-02T00:00:02Z",   "first_name": "Edward",   "age": "forty",   "address": {     "city": "Sunyvale",     "zip": "94003"   } }', GENERATE_UUID(), CURRENT_TIMESTAMP());

CALL testing_evan_2052._airbyte_type_dedupe();

----------------------------
--------- SYNC 3 -----------
----------------------------

CALL testing_evan_2052._airbyte_prepare_raw_table();

-- Load the raw data
-- Delete row 1 with CDC
-- Insert multiple records for a new user (with age incrementing each time)

INSERT INTO testing_evan_2052.users_raw (`_airbyte_data`, `_airbyte_raw_id`, `_airbyte_extracted_at`) VALUES (JSON'{   "id": 1,    "updated_at": "2020-01-03T00:00:00Z",   "first_name": "Brian",   "age": 39,   "address": {     "city": "Menlo Park",     "zip": "99999"   }, "_ab_cdc_deleted_at": true}', GENERATE_UUID(), CURRENT_TIMESTAMP());
INSERT INTO testing_evan_2052.users_raw (`_airbyte_data`, `_airbyte_raw_id`, `_airbyte_extracted_at`) VALUES (JSON'{   "id": 5,    "updated_at": "2020-01-03T00:00:01Z",   "first_name": "Cynthia",   "age": 40,   "address": {     "city": "Redwood City",     "zip": "98765"   }}', GENERATE_UUID(), CURRENT_TIMESTAMP());
INSERT INTO testing_evan_2052.users_raw (`_airbyte_data`, `_airbyte_raw_id`, `_airbyte_extracted_at`) VALUES (JSON'{   "id": 5,    "updated_at": "2020-01-03T00:00:02Z",   "first_name": "Cynthia",   "age": 41,   "address": {     "city": "Redwood City",     "zip": "98765"   }}', GENERATE_UUID(), CURRENT_TIMESTAMP());
INSERT INTO testing_evan_2052.users_raw (`_airbyte_data`, `_airbyte_raw_id`, `_airbyte_extracted_at`) VALUES (JSON'{   "id": 5,    "updated_at": "2020-01-03T00:00:03Z",   "first_name": "Cynthia",   "age": 42,   "address": {     "city": "Redwood City",     "zip": "98765"   }}', GENERATE_UUID(), CURRENT_TIMESTAMP());

CALL testing_evan_2052._airbyte_type_dedupe();

----------------------
-- FINAL VALIDATION --
----------------------
/*

You should see 5 RAW records, one for each of the 5 users
You should see 4 TYPED records, one for each user, except user #2, which was CDC deleted
You should have the latest data for each user in the typed final table:
  * User #1 (Evan) has the latest data (age=39)
  * User #3 (Edward) has a null age [+ error] due to that age being un-typable
  * User #4 (Joe) has a null age & no errors
  * User #5 (Cynthia) has one entry dispite the multiple insertes, with the latest entry (age=42)

*/

SELECT CURRENT_TIMESTAMP();
