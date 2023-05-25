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

DROP TABLE IF EXISTS PUBLIC.USERS;
DROP TABLE IF EXISTS Z_AIRBYTE.USERS_RAW;

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

-------------------------------------
--------- TYPE AND DEDUPE -----------
-------------------------------------

CREATE OR REPLACE PROCEDURE PUBLIC._AIRBYTE_PREPARE_RAW_TABLE()
RETURNS TEXT LANGUAGE SQL AS $$
BEGIN
	CREATE SCHEMA IF NOT EXISTS Z_AIRBYTE;
	CREATE TABLE IF NOT EXISTS Z_AIRBYTE.USERS_RAW (
	    "_airbyte_raw_id" VARCHAR(36) NOT NULL PRIMARY KEY, -- Airbyte column, cannot be null
	    "_airbyte_data" variant NOT NULL, -- Airbyte column, cannot be null
	    "_airbyte_extracted_at" timestamp NOT NULL, -- Airbyte column, cannot be null
	    "_airbyte_loaded_at" timestamp -- Airbyte column
	);
	RETURN 'SUCCESS';
END
$$;

CREATE OR REPLACE PROCEDURE PUBLIC._AIRBYTE_TYPE_DEDUPE()
RETURNS TEXT LANGUAGE SQL AS $$
BEGIN
	-- Moving the data and deduping happens in a transaction to prevent duplicates from appearing
	-- 	BEGIN;

	-- Step 3: Move the new data to the typed table
	INSERT INTO PUBLIC.USERS
	SELECT
		TRY_CAST("_airbyte_data":"id"::text AS INT) as id,
		TRY_CAST("_airbyte_data":"first_name"::text AS TEXT) as first_name,
		TRY_CAST("_airbyte_data":"age"::text AS INT) as age,
		"_airbyte_data":"address" as address, -- TRY_CAST does not work with JSON/VARIANT
		TRY_CAST("_airbyte_data":"updated_at"::text AS TIMESTAMP) as updated_at,
		(
			CASE
				WHEN "_airbyte_data":"id" IS NOT NULL AND TRY_CAST("_airbyte_data":"id"::text AS INT) IS NULL THEN PARSE_JSON('{error: "Problem with `id`"}')
				WHEN "_airbyte_data":"first_name" IS NOT NULL AND TRY_CAST("_airbyte_data":"first_name"::text AS TEXT) IS NULL THEN PARSE_JSON('{error: "Problem with `first_name`"}')
				WHEN "_airbyte_data":"age" IS NOT NULL AND TRY_CAST("_airbyte_data":"age"::text AS INT) IS NULL THEN PARSE_JSON('{error: "Problem with `age`"}')
				-- no TRY_CAST for JSON
				WHEN "_airbyte_data":"updated_at" IS NOT NULL AND TRY_CAST("_airbyte_data":"updated_at"::text AS TIMESTAMP) IS NULL THEN PARSE_JSON('{error: "Problem with `updated_at`"}')
				ELSE PARSE_JSON('{}')
			END
		) as _airbyte_meta,
		"_airbyte_raw_id",
		"_airbyte_extracted_at"
	FROM Z_AIRBYTE.USERS_RAW
	WHERE
		"_airbyte_loaded_at" IS NULL -- inserting only new/null values, we can recover from failed previous checkpoints
		AND "_airbyte_data":"_ab_cdc_deleted_at" IS NULL -- Skip CDC deleted rows (old records are already cleared away above
	;

	-- Step 4: Dedupe and clean the typed table
	-- This is a full table scan, but we need to do it this way to merge the new rows with the old to:
	--   * Consider the case in which there are multiple entries for the same PK in the new insert batch
	--	 * Consider the case in which the data in the new batch is older than the data in the typed table, and we only want to keep the newer (pre-existing) data

	DELETE FROM PUBLIC.USERS
	WHERE
		-- Delete any rows which are not the most recent for a given PK
		"_airbyte_raw_id" IN (
			SELECT "_airbyte_raw_id" FROM (
				SELECT "_airbyte_raw_id", row_number() OVER (
					PARTITION BY "id" ORDER BY "updated_at" DESC, "_airbyte_extracted_at" DESC
				) as row_number FROM PUBLIC.USERS
			)
			WHERE row_number != 1
		)
		OR
		-- Delete rows that have been CDC deleted
		"id" IN (
			SELECT
				TRY_CAST("_airbyte_data":"id"::text AS INT) as id -- based on the PK which we know from the connector catalog
			FROM Z_AIRBYTE.USERS_RAW
			WHERE "_airbyte_data":"_ab_cdc_deleted_at" IS NOT NULL
		)
	;

	-- Step 5: Remove old entries from Raw table
	DELETE FROM Z_AIRBYTE.USERS_RAW
	WHERE
		"_airbyte_raw_id" NOT IN (
			SELECT "_airbyte_raw_id" FROM PUBLIC.USERS
		)
		AND
		"_airbyte_data":"_ab_cdc_deleted_at" IS NULL -- we want to keep the final _ab_cdc_deleted_at=true entry in the raw table for the deleted record
	;

	-- Step 6: Apply typed_at timestamp where needed
	UPDATE Z_AIRBYTE.USERS_RAW
	SET "_airbyte_loaded_at" = CURRENT_TIMESTAMP()
	WHERE "_airbyte_loaded_at" IS NULL
	;

	-- 	COMMIT;
	RETURN 'SUCCESS';
END
$$;

----------------------------
--------- SYNC 1 -----------
----------------------------

CALL PUBLIC._AIRBYTE_PREPARE_RAW_TABLE();

-- Load the raw data

INSERT INTO Z_AIRBYTE.USERS_RAW ("_airbyte_data", "_airbyte_raw_id", "_airbyte_extracted_at") SELECT PARSE_JSON($${
	id: 1,
	first_name: "Evan",
	age: 38,
	updated_at: "2020-01-01T00:00:00Z",
	address:{
		city: "San Francisco",
		zip: "94001"
} }$$), UUID_STRING(), CURRENT_TIMESTAMP();
INSERT INTO Z_AIRBYTE.USERS_RAW ("_airbyte_data", "_airbyte_raw_id", "_airbyte_extracted_at") SELECT PARSE_JSON($${
	id: 2,
	first_name: "Brian",
	age: 39,
	updated_at: "2020-01-01T00:00:01Z",
	address:{
		city: "Menlo Park",
		zip: "94002"
} }$$), UUID_STRING(), CURRENT_TIMESTAMP();
INSERT INTO Z_AIRBYTE.USERS_RAW ("_airbyte_data", "_airbyte_raw_id", "_airbyte_extracted_at") SELECT PARSE_JSON($${
	id: 3,
	first_name: "Edward",
	age: 40,
	updated_at: "2020-01-01T00:00:02Z",
	address:{
		city: "Sunyvale",
		zip: "94003"
} }$$), UUID_STRING(), CURRENT_TIMESTAMP();
-- Joe is missing an age, null OK
INSERT INTO Z_AIRBYTE.USERS_RAW ("_airbyte_data", "_airbyte_raw_id", "_airbyte_extracted_at") SELECT PARSE_JSON($${
	id: 4,
	first_name: "Joe",
	age: 40,
	updated_at: "2020-01-01T00:00:03Z",
	address:{
		city: "Seattle",
		zip: "98999"
} }$$), UUID_STRING(), CURRENT_TIMESTAMP();

CALL PUBLIC._AIRBYTE_TYPE_DEDUPE();

----------------------------
--------- SYNC 2 -----------
----------------------------

CALL PUBLIC._AIRBYTE_PREPARE_RAW_TABLE();

-- Load the raw data
-- Age update for Evan (user 1)
-- There is an update for Brian (user 2, new address.zip)
-- There is an update for Edward (user 3, age is invalid)
-- No update for Joe (user 4)

INSERT INTO Z_AIRBYTE.USERS_RAW ("_airbyte_data", "_airbyte_raw_id", "_airbyte_extracted_at") SELECT PARSE_JSON($${
	id: 1,
	first_name: "Evan",
	age: 39,
	updated_at: "2020-01-02T00:00:00Z",
	address:{
		city: "San Francisco",
		zip: "94001"
} }$$), UUID_STRING(), CURRENT_TIMESTAMP();
INSERT INTO Z_AIRBYTE.USERS_RAW ("_airbyte_data", "_airbyte_raw_id", "_airbyte_extracted_at") SELECT PARSE_JSON($${
	id: 2,
	first_name: "Brian",
	age: 39,
	updated_at: "2020-01-02T00:00:01Z",
	address:{
		city: "Menlo Park",
		zip: "99999"
} }$$), UUID_STRING(), CURRENT_TIMESTAMP();
INSERT INTO Z_AIRBYTE.USERS_RAW ("_airbyte_data", "_airbyte_raw_id", "_airbyte_extracted_at") SELECT PARSE_JSON($${
	id: 3,
	first_name: "Edward",
	age: "forty",
	updated_at: "2020-01-02T00:00:02Z",
	address:{
		city: "Sunyvale",
		zip: "94003"
} }$$), UUID_STRING(), CURRENT_TIMESTAMP();

CALL PUBLIC._AIRBYTE_TYPE_DEDUPE();

----------------------------
--------- SYNC 3 -----------
----------------------------

CALL PUBLIC._AIRBYTE_PREPARE_RAW_TABLE();

-- Step 1: Load the raw data
-- Delete row 1 with CDC

INSERT INTO Z_AIRBYTE.USERS_RAW ("_airbyte_data", "_airbyte_raw_id", "_airbyte_extracted_at") SELECT PARSE_JSON($${
	_ab_cdc_deleted_at: true,
	id: 2,
	first_name: "Brian",
	age: 39,
	updated_at: "2020-01-03T00:00:00Z",
	address:{
		city: "Menlo Park",
		zip: "99999"
} }$$), UUID_STRING(), CURRENT_TIMESTAMP();
INSERT INTO Z_AIRBYTE.USERS_RAW ("_airbyte_data", "_airbyte_raw_id", "_airbyte_extracted_at") SELECT PARSE_JSON($${
	id: 5,
	first_name: "Cynthia",
	age: 40,
	updated_at: "2020-01-03T00:00:01Z",
	address:{
		city: "Redwood City",
		zip: "98765"
} }$$), UUID_STRING(), CURRENT_TIMESTAMP();
INSERT INTO Z_AIRBYTE.USERS_RAW ("_airbyte_data", "_airbyte_raw_id", "_airbyte_extracted_at") SELECT PARSE_JSON($${
	id: 5,
	first_name: "Cynthia",
	age: 41,
	updated_at: "2020-01-03T00:00:02Z",
	address:{
		city: "Redwood City",
		zip: "98765"
} }$$), UUID_STRING(), CURRENT_TIMESTAMP();
INSERT INTO Z_AIRBYTE.USERS_RAW ("_airbyte_data", "_airbyte_raw_id", "_airbyte_extracted_at") SELECT PARSE_JSON($${
	id: 5,
	first_name: "Cynthia",
	age: 42,
	updated_at: "2020-01-03T00:00:03Z",
	address:{
		city: "Redwood City",
		zip: "98765"
} }$$), UUID_STRING(), CURRENT_TIMESTAMP();

CALL PUBLIC._AIRBYTE_TYPE_DEDUPE();

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
