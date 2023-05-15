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
  }]
}
*/

/*

KNOWN LIMITATIONS
* Only one error type shown per row, the first one sequentially
* There's a full table scan used for de-duplication.  This can be made more efficient...
* It would be better to show the actual error message from the DB, not custom "this column is bad" strings

*/

-- Set up the Experiment
-- Assumption: We can build the table at the start of the sync based only on the schema we get from the source/configured catalog

DROP TABLE IF EXISTS PUBLIC.USERS;

CREATE TABLE PUBLIC.USERS (
    "id" int, -- PK cannot be null, but after raw insert and before typing, row will be null
    "first_name" text,
    "age" int,
    "address" variant,
    "_airbyte_meta" variant NOT NULL, -- Airbyte column, cannot be null
    "_airbyte_raw_id" VARCHAR(36) NOT NULL, -- Airbyte column, cannot be null
    "_airbyte_read_at" timestamp NOT NULL -- Airbyte column, cannot be null
);

----------------------------
--------- SYNC 1 -----------
----------------------------

-- Step 0: Prepare the raw table

CREATE SCHEMA IF NOT EXISTS Z_AIRBYTE;
CREATE TABLE IF NOT EXISTS Z_AIRBYTE.USERS_RAW (
    "_airbyte_data" variant NOT NULL, -- Airbyte column, cannot be null
    "_airbyte_raw_id" VARCHAR(36) NOT NULL, -- Airbyte column, cannot be null
    "_airbyte_read_at" timestamp NOT NULL, -- Airbyte column, cannot be null
    "_airbyte_typed_at" timestamp -- Airbyte column
);

-- Step 1: Load the raw data

INSERT INTO Z_AIRBYTE.USERS_RAW ("_airbyte_data", "_airbyte_raw_id", "_airbyte_read_at") SELECT PARSE_JSON($${
	id: 1,
	first_name: "Evan",
	age: 38,
	address:{
		city: "San Francisco",
		zip: "94001"
} }$$), UUID_STRING(), CURRENT_TIMESTAMP();
INSERT INTO Z_AIRBYTE.USERS_RAW ("_airbyte_data", "_airbyte_raw_id", "_airbyte_read_at") SELECT PARSE_JSON($${
	id: 2,
	first_name: "Brian",
	age: 39,
	address:{
		city: "Menlo Park",
		zip: "94002"
} }$$), UUID_STRING(), CURRENT_TIMESTAMP();
INSERT INTO Z_AIRBYTE.USERS_RAW ("_airbyte_data", "_airbyte_raw_id", "_airbyte_read_at") SELECT PARSE_JSON($${
	id: 3,
	first_name: "Edward",
	age: 40,
	address:{
		city: "Sunyvale",
		zip: "94003"
} }$$), UUID_STRING(), CURRENT_TIMESTAMP();
-- Joe is missing an age, null OK
INSERT INTO Z_AIRBYTE.USERS_RAW ("_airbyte_data", "_airbyte_raw_id", "_airbyte_read_at") SELECT PARSE_JSON($${
	id: 4,
	first_name: "Joe",
	age: 40,
	address:{
		city: "Seattle",
		zip: "98999"
} }$$), UUID_STRING(), CURRENT_TIMESTAMP();

-- Moving the data and deduping happens in a transaction to prevent duplicates from appearing
BEGIN;

-- Step 2: First, delete any old entries from the raw table which have new records
-- This might be better than using row_number() after inserting the new data into the raw table because the set of PKs to consider will likely be smaller.  The trade is a second round of SAFE_CAST. if that's fast, it might be a good idea

DELETE FROM Z_AIRBYTE.USERS_RAW
WHERE "_airbyte_raw_id" IN (
	SELECT "_airbyte_raw_id"
	FROM PUBLIC.USERS
	WHERE
		"id" IN (
			SELECT TRY_CAST("_airbyte_data":"id"::text AS INT) as id
			FROM Z_AIRBYTE.USERS_RAW
			WHERE "_airbyte_typed_at" IS NULL -- considering only new/null values, we can recover from failed previous checkpoints
		)
)
AND "_airbyte_typed_at" IS NOT NULL
;

-- Step 3: Also, delete any old entries from the typed table which have new records

DELETE FROM PUBLIC.USERS
WHERE "id" in (
	SELECT
		TRY_CAST("_airbyte_data":"id"::text AS INT) as id
	FROM Z_AIRBYTE.USERS_RAW
	WHERE "_airbyte_typed_at" IS NULL -- considering only new/null values, we can recover from failed previous checkpoints
)
;


-- Step 3: Type the Data & handle errors
-- Note: We know the column names from the schema, so we don't need to anything refelxive to look up the column names
-- Don't insert rows which have been deleted by CDC

INSERT INTO PUBLIC.USERS
SELECT
	TRY_CAST("_airbyte_data":"id"::text AS INT) as id,
	TRY_CAST("_airbyte_data":"first_name"::text AS TEXT) as first_name,
	TRY_CAST("_airbyte_data":"age"::text AS INT) as age,
	"_airbyte_data":"address" as address, -- TRY_CAST does not work with JSON/VARIANT
	(
		CASE
			WHEN "_airbyte_data":"id" IS NOT NULL AND TRY_CAST("_airbyte_data":"id"::text AS INT) IS NULL THEN PARSE_JSON($${error: "Problem with `id`"}$$)
			WHEN "_airbyte_data":"first_name" IS NOT NULL AND TRY_CAST("_airbyte_data":"first_name"::text AS TEXT) IS NULL THEN PARSE_JSON($${error: "Problem with `first_name`"}$$)
			WHEN "_airbyte_data":"age" IS NOT NULL AND TRY_CAST("_airbyte_data":"age"::text AS INT) IS NULL THEN PARSE_JSON($${error: "Problem with `age`"}$$)
			-- no TRY_CAST for JSON
			ELSE PARSE_JSON($${}$$)
		END
	) as _airbyte_meta,
	"_airbyte_raw_id",
	"_airbyte_read_at"
FROM Z_AIRBYTE.USERS_RAW
WHERE
	"_airbyte_typed_at" IS NULL -- inserting only new/null values, we can recover from failed previous checkpoints
	AND "_airbyte_data":"_ab_cdc_deleted_at" IS NULL -- Skip CDC deleted rows (old records are already cleared away above
;

-- Step 4: Apply typed_at timestamp where needed
UPDATE Z_AIRBYTE.USERS_RAW
SET "_airbyte_typed_at" = CURRENT_TIMESTAMP()
WHERE "_airbyte_typed_at" IS NULL
;

COMMIT;

----------------------------
--------- SYNC 2 -----------
----------------------------

-- Step 0: Prepare the raw table

CREATE SCHEMA IF NOT EXISTS Z_AIRBYTE;
CREATE TABLE IF NOT EXISTS Z_AIRBYTE.USERS_RAW (
    "_airbyte_data" variant NOT NULL, -- Airbyte column, cannot be null
    "_airbyte_raw_id" VARCHAR(36) NOT NULL, -- Airbyte column, cannot be null
    "_airbyte_read_at" timestamp NOT NULL, -- Airbyte column, cannot be null
    "_airbyte_typed_at" timestamp -- Airbyte column
);


-- Step 1: Load the raw data
-- No update for Evan (user 1)
-- There is an update for Brian (user 2, new address.zip)
-- There is an update for Edward (user 3, age is invalid)
-- No update for Joe (user 4)

INSERT INTO Z_AIRBYTE.USERS_RAW ("_airbyte_data", "_airbyte_raw_id", "_airbyte_read_at") SELECT PARSE_JSON($${
	id: 2,
	first_name: "Brian",
	age: 39,
	address:{
		city: "Menlo Park",
		zip: "99999"
} }$$), UUID_STRING(), CURRENT_TIMESTAMP();
INSERT INTO Z_AIRBYTE.USERS_RAW ("_airbyte_data", "_airbyte_raw_id", "_airbyte_read_at") SELECT PARSE_JSON($${
	id: 3,
	first_name: "Edward",
	age: "forty",
	address:{
		city: "Sunyvale",
		zip: "94003"
} }$$), UUID_STRING(), CURRENT_TIMESTAMP();

-- Moving the data and deduping happens in a transaction to prevent duplicates from appearing
BEGIN;

-- Step 2: First, delete any old entries from the raw table which have new records
-- This might be better than using row_number() after inserting the new data into the raw table because the set of PKs to consider will likely be smaller.  The trade is a second round of SAFE_CAST. if that's fast, it might be a good idea

DELETE FROM Z_AIRBYTE.USERS_RAW
WHERE "_airbyte_raw_id" IN (
	SELECT "_airbyte_raw_id"
	FROM PUBLIC.USERS
	WHERE
		"id" IN (
			SELECT TRY_CAST("_airbyte_data":"id"::text AS INT) as id
			FROM Z_AIRBYTE.USERS_RAW
			WHERE "_airbyte_typed_at" IS NULL -- considering only new/null values, we can recover from failed previous checkpoints
		)
)
AND "_airbyte_typed_at" IS NOT NULL
;

-- Step 3: Also, delete any old entries from the typed table which have new records

DELETE FROM PUBLIC.USERS
WHERE "id" in (
	SELECT
		TRY_CAST("_airbyte_data":"id"::text AS INT) as id
	FROM Z_AIRBYTE.USERS_RAW
	WHERE "_airbyte_typed_at" IS NULL -- considering only new/null values, we can recover from failed previous checkpoints
)
;


-- Step 3: Type the Data & handle errors
-- Note: We know the column names from the schema, so we don't need to anything refelxive to look up the column names
-- Don't insert rows which have been deleted by CDC

INSERT INTO PUBLIC.USERS
SELECT
	TRY_CAST("_airbyte_data":"id"::text AS INT) as id,
	TRY_CAST("_airbyte_data":"first_name"::text AS TEXT) as first_name,
	TRY_CAST("_airbyte_data":"age"::text AS INT) as age,
	"_airbyte_data":"address" as address, -- TRY_CAST does not work with JSON/VARIANT
	(
		CASE
			WHEN "_airbyte_data":"id" IS NOT NULL AND TRY_CAST("_airbyte_data":"id"::text AS INT) IS NULL THEN PARSE_JSON($${error: "Problem with `id`"}$$)
			WHEN "_airbyte_data":"first_name" IS NOT NULL AND TRY_CAST("_airbyte_data":"first_name"::text AS TEXT) IS NULL THEN PARSE_JSON($${error: "Problem with `first_name`"}$$)
			WHEN "_airbyte_data":"age" IS NOT NULL AND TRY_CAST("_airbyte_data":"age"::text AS INT) IS NULL THEN PARSE_JSON($${error: "Problem with `age`"}$$)
			-- no TRY_CAST for JSON
			ELSE PARSE_JSON($${}$$)
		END
	) as _airbyte_meta,
	"_airbyte_raw_id",
	"_airbyte_read_at"
FROM Z_AIRBYTE.USERS_RAW
WHERE
	"_airbyte_typed_at" IS NULL -- inserting only new/null values, we can recover from failed previous checkpoints
	AND "_airbyte_data":"_ab_cdc_deleted_at" IS NULL -- Skip CDC deleted rows (old records are already cleared away above
;

-- Step 4: Apply typed_at timestamp where needed
UPDATE Z_AIRBYTE.USERS_RAW
SET "_airbyte_typed_at" = CURRENT_TIMESTAMP()
WHERE "_airbyte_typed_at" IS NULL
;

COMMIT;

----------------------------
--------- SYNC 3 -----------
----------------------------

-- Step 0: Prepare the raw table

CREATE SCHEMA IF NOT EXISTS Z_AIRBYTE;
CREATE TABLE IF NOT EXISTS Z_AIRBYTE.USERS_RAW (
    "_airbyte_data" variant NOT NULL, -- Airbyte column, cannot be null
    "_airbyte_raw_id" VARCHAR(36) NOT NULL, -- Airbyte column, cannot be null
    "_airbyte_read_at" timestamp NOT NULL, -- Airbyte column, cannot be null
    "_airbyte_typed_at" timestamp -- Airbyte column
);


-- Step 1: Load the raw data
-- Delete row 1 with CDC

INSERT INTO Z_AIRBYTE.USERS_RAW ("_airbyte_data", "_airbyte_raw_id", "_airbyte_read_at") SELECT PARSE_JSON($${
	_ab_cdc_deleted_at: true,
	id: 2,
	first_name: "Brian",
	age: 39,
	address:{
		city: "Menlo Park",
		zip: "99999"
} }$$), UUID_STRING(), CURRENT_TIMESTAMP();

-- Moving the data and deduping happens in a transaction to prevent duplicates from appearing
BEGIN;

-- Step 2: First, delete any old entries from the raw table which have new records
-- This might be better than using row_number() after inserting the new data into the raw table because the set of PKs to consider will likely be smaller.  The trade is a second round of SAFE_CAST. if that's fast, it might be a good idea

DELETE FROM Z_AIRBYTE.USERS_RAW
WHERE "_airbyte_raw_id" IN (
	SELECT "_airbyte_raw_id"
	FROM PUBLIC.USERS
	WHERE
		"id" IN (
			SELECT TRY_CAST("_airbyte_data":"id"::text AS INT) as id
			FROM Z_AIRBYTE.USERS_RAW
			WHERE "_airbyte_typed_at" IS NULL -- considering only new/null values, we can recover from failed previous checkpoints
		)
)
AND "_airbyte_typed_at" IS NOT NULL
;

-- Step 3: Also, delete any old entries from the typed table which have new records

DELETE FROM PUBLIC.USERS
WHERE "id" in (
	SELECT
		TRY_CAST("_airbyte_data":"id"::text AS INT) as id
	FROM Z_AIRBYTE.USERS_RAW
	WHERE "_airbyte_typed_at" IS NULL -- considering only new/null values, we can recover from failed previous checkpoints
)
;


-- Step 3: Type the Data & handle errors
-- Note: We know the column names from the schema, so we don't need to anything refelxive to look up the column names
-- Don't insert rows which have been deleted by CDC

INSERT INTO PUBLIC.USERS
SELECT
	TRY_CAST("_airbyte_data":"id"::text AS INT) as id,
	TRY_CAST("_airbyte_data":"first_name"::text AS TEXT) as first_name,
	TRY_CAST("_airbyte_data":"age"::text AS INT) as age,
	"_airbyte_data":"address" as address, -- TRY_CAST does not work with JSON/VARIANT
	(
		CASE
			WHEN "_airbyte_data":"id" IS NOT NULL AND TRY_CAST("_airbyte_data":"id"::text AS INT) IS NULL THEN PARSE_JSON($${error: "Problem with `id`"}$$)
			WHEN "_airbyte_data":"first_name" IS NOT NULL AND TRY_CAST("_airbyte_data":"first_name"::text AS TEXT) IS NULL THEN PARSE_JSON($${error: "Problem with `first_name`"}$$)
			WHEN "_airbyte_data":"age" IS NOT NULL AND TRY_CAST("_airbyte_data":"age"::text AS INT) IS NULL THEN PARSE_JSON($${error: "Problem with `age`"}$$)
			-- no TRY_CAST for JSON
			ELSE PARSE_JSON($${}$$)
		END
	) as _airbyte_meta,
	"_airbyte_raw_id",
	"_airbyte_read_at"
FROM Z_AIRBYTE.USERS_RAW
WHERE
	"_airbyte_typed_at" IS NULL -- inserting only new/null values, we can recover from failed previous checkpoints
	AND "_airbyte_data":"_ab_cdc_deleted_at" IS NULL -- Skip CDC deleted rows (old records are already cleared away above
;

-- Step 4: Apply typed_at timestamp where needed
UPDATE Z_AIRBYTE.USERS_RAW
SET "_airbyte_typed_at" = CURRENT_TIMESTAMP()
WHERE "_airbyte_typed_at" IS NULL
;

COMMIT;
