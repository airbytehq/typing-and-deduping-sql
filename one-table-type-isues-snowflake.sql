DROP TABLE IF EXISTS PUBLIC.USERS;
DROP TABLE IF EXISTS Z_AIRBYTE.USERS_RAW;

CREATE TABLE PUBLIC.USERS (
    "id" int PRIMARY KEY -- PK cannot be null, but after raw insert and before typing, row will be null
    ,"first_name" text
    ,"age" int
    ,"address" variant
  	,"updated_at" timestamp -- NOT NULL
    ,"_airbyte_meta" variant NOT NULL -- Airbyte column, cannot be null
    ,"_airbyte_raw_id" VARCHAR(36) NOT NULL -- Airbyte column, cannot be null
    ,"_airbyte_extracted_at" timestamp NOT NULL -- Airbyte column, cannot be null
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

---------------------
-- BAD DATA CHECKS --
---------------------

CALL PUBLIC._AIRBYTE_PREPARE_RAW_TABLE();

/*
Problem: Writing a decimal to an int column
Outcome: Snowflake truncates the decimal to an int, no error
*/

INSERT INTO Z_AIRBYTE.USERS_RAW ("_airbyte_data", "_airbyte_raw_id", "_airbyte_extracted_at") SELECT PARSE_JSON($${
	id: 101,
	first_name: "bad-decimal-number-to-int",
	age: 0.1,
	updated_at: "2020-01-02T00:00:00Z",
	address:{
		city: "city",
		zip: "01234"
} }$$), UUID_STRING(), CURRENT_TIMESTAMP();

/*
Problem: Writing a negative number to an unsigned int column
Outcome: Snowflake doesn't make a distinction about signed vs unsigned, no error
*/

INSERT INTO Z_AIRBYTE.USERS_RAW ("_airbyte_data", "_airbyte_raw_id", "_airbyte_extracted_at") SELECT PARSE_JSON($${
	id: 102,
	first_name: "bad-negative-int-to-int",
	age: -1,
	updated_at: "2020-01-02T00:00:00Z",
	address:{
		city: "city",
		zip: "01234"
} }$$), UUID_STRING(), CURRENT_TIMESTAMP();

/*
Problem: Writing a number lager than snowflake can handle to an int column
Outcome: NULL, typing error caught
Learn more: (99999999999999999999999999999999999999 + 1) https://docs.snowflake.com/en/sql-reference/data-types-numeric
*/

INSERT INTO Z_AIRBYTE.USERS_RAW ("_airbyte_data", "_airbyte_raw_id", "_airbyte_extracted_at") SELECT PARSE_JSON($${
	id: 103,
	first_name: "bad-int-too-big",
	age: 9999999999999999999999999999999999999900000,
	updated_at: "2020-01-02T00:00:00Z",
	address:{
		city: "city",
		zip: "01234"
} }$$), UUID_STRING(), CURRENT_TIMESTAMP();

/*
Problem: Writing a negative (B.C.E.) date value
Outcome: NULL, typing error caught
*/

INSERT INTO Z_AIRBYTE.USERS_RAW ("_airbyte_data", "_airbyte_raw_id", "_airbyte_extracted_at") SELECT PARSE_JSON($${
	id: 104,
	first_name: "bad-negative-date",
	age: 123,
	updated_at: "-2020-01-02T00:00:00Z",
	address:{
		city: "city",
		zip: "01234"
} }$$), UUID_STRING(), CURRENT_TIMESTAMP();

/*
Problem: Writing bas JSON to a JSON column
Outcome: 'foo', there isn't a "safe_cast" for JSON in snowflake, so it's fine.  We might want to validate this manually somehow.
*/

INSERT INTO Z_AIRBYTE.USERS_RAW ("_airbyte_data", "_airbyte_raw_id", "_airbyte_extracted_at") SELECT PARSE_JSON($${
	id: 105,
	first_name: "bad-sub-json",
	age: 123,
	updated_at: "2020-01-02T00:00:00Z",
	address:'foo'
}$$), UUID_STRING(), CURRENT_TIMESTAMP();

CALL PUBLIC._AIRBYTE_TYPE_DEDUPE();

----------------------
-- FINAL VALIDATION --
----------------------

/*
There should be no errors.  All rows above should have made it to the final table
*/

SELECT CURRENT_TIMESTAMP();
