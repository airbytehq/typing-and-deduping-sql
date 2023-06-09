/*
SQL Experiments for Typing and Normalizing AirbyteRecords in 1 table
Run me on Postgres

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

DROP TABLE IF EXISTS evan.users;
DROP TABLE IF EXISTS evan.users_raw;

CREATE TABLE evan.users (
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
	-- TODO: Learn about partition_expiration_days https://cloud.google.com/bigquery/docs/creating-partitioned-tables
) OPTIONS (
	description="users table"
)
;

-------------------------------------
--------- TYPE AND DEDUPE -----------
-------------------------------------

CREATE OR REPLACE PROCEDURE evan._airbyte_prepare_raw_table()
BEGIN
	CREATE TABLE IF NOT EXISTS evan.users_raw (
	      `_airbyte_raw_id` STRING NOT NULL OPTIONS (description = 'Airbyte column, cannot be null')
	    , `_airbyte_data` JSON NOT NULL OPTIONS (description = 'Airbyte column, cannot be null')
	    , `_airbyte_extracted_at` TIMESTAMP NOT NULL OPTIONS (description = 'Airbyte column, cannot be null')
	    , `_airbyte_loaded_at` TIMESTAMP
	)
	PARTITION BY (
		DATE_TRUNC(_airbyte_extracted_at, MONTH)
	)
	;
END
;


CREATE OR REPLACE PROCEDURE evan._airbyte_type_dedupe()
BEGIN

	-- Step 1: Validate the incoming data
	-- We can't really do this properly in the pure-SQL example here, but we should throw if any row doesn't have a PK
	DECLARE missing_pk_count INT64;
	SET missing_pk_count = (
		SELECT COUNT(1)
		FROM evan.users_raw
		WHERE
			`_airbyte_loaded_at` IS NULL
			AND SAFE_CAST(JSON_EXTRACT_SCALAR(`_airbyte_data`, '$.id') as INT64) IS NULL
		);

	IF missing_pk_count > 0 THEN
		RAISE USING message = FORMAT("Raw table has %s rows missing a primary key", CAST(missing_pk_count AS STRING));
	END IF;

	-- Moving the data and deduping happens in a transaction to prevent duplicates from appearing
	-- BEGIN

	-- Step 2: Move the new data to the typed table
	INSERT INTO evan.users
	SELECT
		SAFE_CAST(JSON_EXTRACT_SCALAR(`_airbyte_data`, '$.id') as INT64) as id,
		SAFE_CAST(JSON_EXTRACT_SCALAR(`_airbyte_data`, '$.first_name') as STRING) as first_name,
		SAFE_CAST(JSON_EXTRACT_SCALAR(`_airbyte_data`, '$.age') as INT64) as age,
		SAFE_CAST(JSON_QUERY(`_airbyte_data`, '$.address') as JSON) as address, -- NOTE: For record properties remaining as JSON, you `JSON_QUERY`, not `JSON_EXTRACT_SCALAR`
		SAFE_CAST(JSON_EXTRACT_SCALAR(`_airbyte_data`, '$.updated_at') as TIMESTAMP) as updated_at,
		CASE
			WHEN (JSON_EXTRACT_SCALAR(`_airbyte_data`, '$.id') IS NOT NULL) AND (SAFE_CAST(JSON_EXTRACT_SCALAR(`_airbyte_data`, '$.id') as INT64) IS NULL) THEN JSON'{"error": "Problem with `id`"}'
			WHEN (JSON_EXTRACT_SCALAR(`_airbyte_data`, '$.first_name') IS NOT NULL) AND (SAFE_CAST(JSON_EXTRACT_SCALAR(`_airbyte_data`, '$.first_name') as STRING) IS NULL) THEN JSON'{"error": "Problem with `first_name`"}'
			WHEN (JSON_EXTRACT_SCALAR(`_airbyte_data`, '$.age') IS NOT NULL) AND (SAFE_CAST(JSON_EXTRACT_SCALAR(`_airbyte_data`, '$.age') as INT64) IS NULL) THEN JSON'{"error": "Problem with `age`"}'
			WHEN (JSON_EXTRACT_SCALAR(`_airbyte_data`, '$.updated_at') IS NOT NULL) AND (SAFE_CAST(JSON_EXTRACT_SCALAR(`_airbyte_data`, '$.updated_at') as TIMESTAMP) IS NULL) THEN JSON'{"error": "Problem with `updated_at`"}'
			WHEN (JSON_QUERY(`_airbyte_data`, '$.address') IS NOT NULL) AND (SAFE_CAST(JSON_QUERY(`_airbyte_data`, '$.address') as JSON) IS NULL) THEN JSON'{"error": "Problem with `address`"}'
			ELSE JSON'{}'
		END as _airbyte_meta,
		_airbyte_raw_id,
		_airbyte_extracted_at
	FROM evan.users_raw
	WHERE
		_airbyte_loaded_at IS NULL -- inserting only new/null values, we can recover from failed previous checkpoints
		AND JSON_EXTRACT(`_airbyte_data`, '$._ab_cdc_deleted_at') IS NULL -- Skip CDC deleted rows (old records are already cleared away above
	;

	-- Step 3: Dedupe and clean the typed table
	-- This is a full table scan, but we need to do it this way to merge the new rows with the old to:
	--   * Consider the case in which there are multiple entries for the same PK in the new insert batch
	--	 * Consider the case in which the data in the new batch is older than the data in the typed table, and we only want to keep the newer (pre-existing) data
	--   * Order by the source's provided cursor and _airbyte_extracted_at to break any ties

	-- NOTE: Bigquery does not support using CTEs for delete statements: https://stackoverflow.com/questions/48130324/bigquery-delete-statement-to-remove-duplicates

	DELETE FROM evan.users
	WHERE
		-- Delete any rows which are not the most recent for a given PK
		`_airbyte_raw_id` IN (
			SELECT `_airbyte_raw_id` FROM (
				SELECT `_airbyte_raw_id`, row_number() OVER (
					PARTITION BY `id` ORDER BY `updated_at` DESC, `_airbyte_extracted_at` DESC
				) as row_number FROM evan.users
			)
			WHERE row_number != 1
		)
		OR
		-- Delete rows that have been CDC deleted
		`id` IN (
			SELECT
				SAFE_CAST(JSON_EXTRACT_SCALAR(`_airbyte_data`, '$.id') as INT64) as id -- based on the PK which we know from the connector catalog
			FROM evan.users_raw
			WHERE JSON_EXTRACT_SCALAR(`_airbyte_data`, '$._ab_cdc_deleted_at') IS NOT NULL
		)
	;

	-- Step 4: Remove old entries from Raw table
	DELETE FROM
		evan.users_raw
	WHERE
		`_airbyte_raw_id` NOT IN (
			SELECT `_airbyte_raw_id` FROM evan.users
		)
		AND
		JSON_EXTRACT_SCALAR(`_airbyte_data`, '$._ab_cdc_deleted_at') IS NULL -- we want to keep the final _ab_cdc_deleted_at=true entry in the raw table for the deleted record
	;

	-- Step 5: Apply typed_at timestamp where needed
	UPDATE evan.users_raw
	SET `_airbyte_loaded_at` = CURRENT_TIMESTAMP()
	WHERE `_airbyte_loaded_at` IS NULL
	;

END;

----------------------------
--------- SYNC 1 -----------
----------------------------

CALL evan._airbyte_prepare_raw_table();

-- Load the raw data

INSERT INTO evan.users_raw (`_airbyte_data`, `_airbyte_raw_id`, `_airbyte_extracted_at`) VALUES (JSON'{   "id": 1,    "updated_at": "2020-01-01T00:00:00Z",   "first_name": "Evan",   "age": 38,   "address": {     "city": "San Francisco",     "zip": "94001"   } }', GENERATE_UUID(), CURRENT_TIMESTAMP());
INSERT INTO evan.users_raw (`_airbyte_data`, `_airbyte_raw_id`, `_airbyte_extracted_at`) VALUES (JSON'{   "id": 2,    "updated_at": "2020-01-01T00:00:01Z",   "first_name": "Brian",   "age": 39,   "address": {     "city": "Menlo Park",     "zip": "94002"   } }', GENERATE_UUID(), CURRENT_TIMESTAMP());
INSERT INTO evan.users_raw (`_airbyte_data`, `_airbyte_raw_id`, `_airbyte_extracted_at`) VALUES (JSON'{   "id": 3,    "updated_at": "2020-01-01T00:00:02Z",   "first_name": "Edward",   "age": 40,   "address": {     "city": "Sunyvale",     "zip": "94003"   } }', GENERATE_UUID(), CURRENT_TIMESTAMP());
INSERT INTO evan.users_raw (`_airbyte_data`, `_airbyte_raw_id`, `_airbyte_extracted_at`) VALUES (JSON'{   "id": 4,    "updated_at": "2020-01-01T00:00:03Z",   "first_name": "Joe",   "address": {     "city": "Seattle",     "zip": "98999"   } }', GENERATE_UUID(), CURRENT_TIMESTAMP());


CALL evan._airbyte_type_dedupe();

----------------------------
--------- SYNC 2 -----------
----------------------------

CALL evan._airbyte_prepare_raw_table();

-- Load the raw data
-- Age update for Evan (user 1)
-- There is an update for Brian (user 2, new address.zip)
-- There is an update for Edward (user 3, age is invalid)
-- No update for Joe (user 4)

INSERT INTO evan.users_raw (`_airbyte_data`, `_airbyte_raw_id`, `_airbyte_extracted_at`) VALUES (JSON'{   "id": 1,    "updated_at": "2020-01-02T00:00:00Z",   "first_name": "Evan",   "age": 39,   "address": {     "city": "San Francisco",     "zip": "94001"   } }', GENERATE_UUID(), CURRENT_TIMESTAMP());
INSERT INTO evan.users_raw (`_airbyte_data`, `_airbyte_raw_id`, `_airbyte_extracted_at`) VALUES (JSON'{   "id": 2,    "updated_at": "2020-01-02T00:00:01Z",   "first_name": "Brian",   "age": 39,   "address": {     "city": "Menlo Park",     "zip": "99999"   } }', GENERATE_UUID(), CURRENT_TIMESTAMP());
INSERT INTO evan.users_raw (`_airbyte_data`, `_airbyte_raw_id`, `_airbyte_extracted_at`) VALUES (JSON'{   "id": 3,    "updated_at": "2020-01-02T00:00:02Z",   "first_name": "Edward",   "age": "forty",   "address": {     "city": "Sunyvale",     "zip": "94003"   } }', GENERATE_UUID(), CURRENT_TIMESTAMP());

CALL evan._airbyte_type_dedupe();

----------------------------
--------- SYNC 3 -----------
----------------------------

CALL evan._airbyte_prepare_raw_table();

-- Load the raw data
-- Delete row 1 with CDC
-- Insert multiple records for a new user (with age incrementing each time)

INSERT INTO evan.users_raw (`_airbyte_data`, `_airbyte_raw_id`, `_airbyte_extracted_at`) VALUES (JSON'{   "id": 1,    "updated_at": "2020-01-03T00:00:00Z",   "first_name": "Brian",   "age": 39,   "address": {     "city": "Menlo Park",     "zip": "99999"   }, "_ab_cdc_deleted_at": true}', GENERATE_UUID(), CURRENT_TIMESTAMP());
INSERT INTO evan.users_raw (`_airbyte_data`, `_airbyte_raw_id`, `_airbyte_extracted_at`) VALUES (JSON'{   "id": 5,    "updated_at": "2020-01-03T00:00:01Z",   "first_name": "Cynthia",   "age": 40,   "address": {     "city": "Redwood City",     "zip": "98765"   }}', GENERATE_UUID(), CURRENT_TIMESTAMP());
INSERT INTO evan.users_raw (`_airbyte_data`, `_airbyte_raw_id`, `_airbyte_extracted_at`) VALUES (JSON'{   "id": 5,    "updated_at": "2020-01-03T00:00:02Z",   "first_name": "Cynthia",   "age": 41,   "address": {     "city": "Redwood City",     "zip": "98765"   }}', GENERATE_UUID(), CURRENT_TIMESTAMP());
INSERT INTO evan.users_raw (`_airbyte_data`, `_airbyte_raw_id`, `_airbyte_extracted_at`) VALUES (JSON'{   "id": 5,    "updated_at": "2020-01-03T00:00:03Z",   "first_name": "Cynthia",   "age": 42,   "address": {     "city": "Redwood City",     "zip": "98765"   }}', GENERATE_UUID(), CURRENT_TIMESTAMP());

CALL evan._airbyte_type_dedupe();

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