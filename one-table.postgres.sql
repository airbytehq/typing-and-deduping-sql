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

DROP TABLE IF EXISTS public.users;

CREATE TABLE public.users (
    "id" int8, -- PK cannot be null, but after raw insert and before typing, row will be null
    "first_name" text,
    "age" int8,
    "address" json,
    "_airbyte_meta" json NOT NULL, -- Airbyte column, cannot be null
    "_airbyte_raw_id" uuid NOT NULL, -- Airbyte column, cannot be null
    "_airbyte_read_at" timestamp NOT NULL -- Airbyte column, cannot be null
);

-- indexes for colums we will use
CREATE INDEX "idx_users__airbyte_read_at" ON public.users USING BTREE ("_airbyte_read_at");
CREATE INDEX "idx_users__airbyte_raw_id" ON public.users USING BTREE ("_airbyte_raw_id");
CREATE INDEX "idx_users_pk" ON public.users USING BTREE ("id");

-- SET UP "safe cast" methods
-- Some DBs (BQ) have this built in, but we can do more-or-less the same thing with custom functions

DROP FUNCTION IF EXISTS "public"._airbyte_safe_cast_to_integer(v_input text);

CREATE OR REPLACE FUNCTION _airbyte_safe_cast_to_integer(v_input text)
RETURNS INTEGER AS $$
DECLARE v_int_value INTEGER DEFAULT NULL;
BEGIN
    BEGIN
        v_int_value := v_input::INTEGER;
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Invalid integer value: "%".  Returning NULL.', v_input;
        RETURN NULL;
    END;
RETURN v_int_value;
END;
$$ LANGUAGE plpgsql;

DROP FUNCTION IF EXISTS "public"._airbyte_safe_cast_to_boolean(v_input text);

CREATE OR REPLACE FUNCTION _airbyte_safe_cast_to_boolean(v_input text)
RETURNS BOOLEAN AS $$
DECLARE v_bool_value BOOLEAN DEFAULT NULL;
BEGIN
    BEGIN
        v_bool_value := v_input::BOOLEAN;
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Invalid boolean value: "%".  Returning NULL.', v_input;
        RETURN NULL;
    END;
RETURN v_bool_value;
END;
$$ LANGUAGE plpgsql;

DROP FUNCTION IF EXISTS "public"._airbyte_safe_cast_to_text(v_input text);

CREATE OR REPLACE FUNCTION _airbyte_safe_cast_to_text(v_input text)
RETURNS TEXT AS $$
DECLARE v_text_value TEXT DEFAULT NULL;
BEGIN
    BEGIN
        v_text_value := v_input::TEXT;
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Invalid text value: "%".  Returning NULL.', v_input;
        RETURN NULL;
    END;
RETURN v_text_value;
END;
$$ LANGUAGE plpgsql;

DROP FUNCTION IF EXISTS "public"._airbyte_safe_cast_to_json(v_input text);

CREATE OR REPLACE FUNCTION _airbyte_safe_cast_to_json(v_input text)
RETURNS JSON AS $$
DECLARE v_json_value JSON DEFAULT NULL;
BEGIN
    BEGIN
        v_json_value := v_input::JSON;
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Invalid json value: "%".  Returning NULL.', v_input;
        RETURN NULL;
    END;
RETURN v_json_value;
END;
$$ LANGUAGE plpgsql;

----------------------------
--------- SYNC 1 -----------
----------------------------

-- Step 0: Prepare the raw table

CREATE SCHEMA IF NOT EXISTS z_airbyte;
CREATE TABLE IF NOT EXISTS z_airbyte.users_raw (
    "_airbyte_data" json NOT NULL, -- Airbyte column, cannot be null
    "_airbyte_raw_id" uuid NOT NULL, -- Airbyte column, cannot be null
    "_airbyte_read_at" timestamp NOT NULL, -- Airbyte column, cannot be null
    "_airbyte_typed_at" timestamp -- Airbyte column
);
CREATE INDEX IF NOT EXISTS "idx_users_raw__airbyte_raw_id" ON public.users USING BTREE ("_airbyte_raw_id");
CREATE INDEX IF NOT EXISTS "idx_users_raw__airbyte_read_at" ON public.users USING BTREE ("_airbyte_read_at");
CREATE INDEX IF NOT EXISTS "idx_users_raw__airbyte_typed_at" ON public.users USING BTREE ("_airbyte_typed_at");

-- Step 1: Load the raw data

INSERT INTO z_airbyte.users_raw ("_airbyte_data", "_airbyte_raw_id", "_airbyte_read_at") VALUES ('{   "id": 1,   "first_name": "Evan",   "age": 38,   "address": {     "city": "San Francisco",     "zip": "94001"   } }', gen_random_uuid(), NOW());
INSERT INTO z_airbyte.users_raw ("_airbyte_data", "_airbyte_raw_id", "_airbyte_read_at") VALUES ('{   "id": 2,   "first_name": "Brian",   "age": 39,   "address": {     "city": "Menlo Park",     "zip": "94002"   } }', gen_random_uuid(), NOW());
INSERT INTO z_airbyte.users_raw ("_airbyte_data", "_airbyte_raw_id", "_airbyte_read_at") VALUES ('{   "id": 3,   "first_name": "Edward",   "age": 40,   "address": {     "city": "Sunyvale",     "zip": "94003"   } }', gen_random_uuid(), NOW());
INSERT INTO z_airbyte.users_raw ("_airbyte_data", "_airbyte_raw_id", "_airbyte_read_at") VALUES ('{   "id": 4,   "first_name": "Joe",   "address": {     "city": "Seattle",     "zip": "98999"   } }', gen_random_uuid(), NOW()); -- Joe is missing an age, null OK

-- Moving the data and deduping happens in a transaction to prevent duplicates from appearing
BEGIN;

-- Step 2: Type the Data & handle errors
-- Note: We know the column names from the schema, so we don't need to anything refelxive to look up the column names

INSERT INTO public.users
SELECT
	_airbyte_safe_cast_to_integer(_airbyte_data ->> 'id') as id,
	_airbyte_safe_cast_to_text(_airbyte_data ->> 'first_name') as first_name,
	_airbyte_safe_cast_to_integer(_airbyte_data ->> 'age') as age,
	_airbyte_safe_cast_to_json(_airbyte_data ->> 'address') as address,
	CASE
		WHEN (_airbyte_data ->> 'id' IS NOT NULL) AND (_airbyte_safe_cast_to_integer(_airbyte_data ->> 'id') IS NULL) THEN '{"error": "Problem with `id`"}'
		WHEN (_airbyte_data ->> 'first_name' IS NOT NULL) AND (_airbyte_safe_cast_to_text(_airbyte_data ->> 'first_name') IS NULL) THEN '{"error": "Problem with `first_name`"}'
		WHEN (_airbyte_data ->> 'age' IS NOT NULL) AND (_airbyte_safe_cast_to_integer(_airbyte_data ->> 'age') IS NULL) THEN '{"error": "Problem with `age`"}'
		WHEN (_airbyte_data ->> 'address' IS NOT NULL) AND (_airbyte_safe_cast_to_json(_airbyte_data ->> 'address') IS NULL) THEN '{"error": "Problem with `address`"}'
		ELSE '{}'
	END::JSON as _airbyte_meta,
	_airbyte_raw_id,
	_airbyte_read_at
FROM z_airbyte.users_raw
WHERE
	_airbyte_typed_at IS NULL -- inserting only null values, we can recover from failed previous checkpoints
;

-- Step 3: De-dupe Typed Table
-- ... assuming the user wanted dedupe
-- NOTE: Postgres doesn't "need" _airbyte_raw_id and could use ctid, but to match cloud DWs, that's what we use in this example
WITH cte AS (
	SELECT _airbyte_raw_id, row_number() OVER (
		PARTITION BY id ORDER BY _airbyte_read_at DESC
	) as row_number FROM public.users
)

DELETE FROM public.users
WHERE _airbyte_raw_id in (
	SELECT _airbyte_raw_id FROM cte WHERE row_number != 1
)
;

-- Step 4: Deal with CDC Deletion (and other special cases)
DELETE FROM public.users WHERE id IN (
	SELECT
		_airbyte_safe_cast_to_integer(_airbyte_data ->> 'id') as id -- based on the PK which we know from the connector catalog
	FROM z_airbyte.users_raw
	WHERE _airbyte_data ->> '_ab_cdc_deleted_at' IS NOT NULL
)
;

-- Step 5: Remove old entries from Raw table
DELETE FROM z_airbyte.users_raw
WHERE _airbyte_raw_id NOT IN (
	SELECT _airbyte_raw_id FROM public.users
)
;

-- Step 6: Apply typed_at timestamp where needed
UPDATE z_airbyte.users_raw
SET _airbyte_typed_at = NOW()
WHERE _airbyte_typed_at IS NULL
;


COMMIT;

----------------------------
--------- SYNC 2 -----------
----------------------------

-- Step 0: Prepare the raw table

CREATE SCHEMA IF NOT EXISTS z_airbyte;
CREATE TABLE IF NOT EXISTS z_airbyte.users_raw (
    "_airbyte_data" json NOT NULL, -- Airbyte column, cannot be null
    "_airbyte_raw_id" uuid NOT NULL, -- Airbyte column, cannot be null
    "_airbyte_read_at" timestamp NOT NULL, -- Airbyte column, cannot be null
    "_airbyte_typed_at" timestamp -- Airbyte column
);
CREATE INDEX IF NOT EXISTS "idx_users_raw__airbyte_raw_id" ON public.users USING BTREE ("_airbyte_raw_id");
CREATE INDEX IF NOT EXISTS "idx_users_raw__airbyte_read_at" ON public.users USING BTREE ("_airbyte_read_at");
CREATE INDEX IF NOT EXISTS "idx_users_raw__airbyte_typed_at" ON public.users USING BTREE ("_airbyte_typed_at");

-- Step 1: Load the raw data
-- No update for Evan (user 1)
-- There is an update for Brian (user 2, new address.zip)
-- There is an update for Edward (user 3, age is invalid)
-- No update for Joe (user 4)

INSERT INTO z_airbyte.users_raw ("_airbyte_data", "_airbyte_raw_id", "_airbyte_read_at") VALUES ('{   "id": 2,   "first_name": "Brian",   "age": 39,   "address": {     "city": "Menlo Park",     "zip": "99999"   } }', gen_random_uuid(), NOW());
INSERT INTO z_airbyte.users_raw ("_airbyte_data", "_airbyte_raw_id", "_airbyte_read_at") VALUES ('{   "id": 3,   "first_name": "Edward",   "age": "forty",   "address": {     "city": "Sunyvale",     "zip": "94003"   } }', gen_random_uuid(), NOW());

-- Moving the data and deduping happens in a transaction to prevent duplicates from appearing
BEGIN;

-- Step 2: Type the Data & handle errors
-- Note: We know the column names from the schema, so we don't need to anything refelxive to look up the column names

INSERT INTO public.users
SELECT
	_airbyte_safe_cast_to_integer(_airbyte_data ->> 'id') as id,
	_airbyte_safe_cast_to_text(_airbyte_data ->> 'first_name') as first_name,
	_airbyte_safe_cast_to_integer(_airbyte_data ->> 'age') as age,
	_airbyte_safe_cast_to_json(_airbyte_data ->> 'address') as address,
	CASE
		WHEN (_airbyte_data ->> 'id' IS NOT NULL) AND (_airbyte_safe_cast_to_integer(_airbyte_data ->> 'id') IS NULL) THEN '{"error": "Problem with `id`"}'
		WHEN (_airbyte_data ->> 'first_name' IS NOT NULL) AND (_airbyte_safe_cast_to_text(_airbyte_data ->> 'first_name') IS NULL) THEN '{"error": "Problem with `first_name`"}'
		WHEN (_airbyte_data ->> 'age' IS NOT NULL) AND (_airbyte_safe_cast_to_integer(_airbyte_data ->> 'age') IS NULL) THEN '{"error": "Problem with `age`"}'
		WHEN (_airbyte_data ->> 'address' IS NOT NULL) AND (_airbyte_safe_cast_to_json(_airbyte_data ->> 'address') IS NULL) THEN '{"error": "Problem with `address`"}'
		ELSE '{}'
	END::JSON as _airbyte_meta,
	_airbyte_raw_id,
	_airbyte_read_at
FROM z_airbyte.users_raw
WHERE
	_airbyte_typed_at IS NULL -- inserting only null values, we can recover from failed previous checkpoints
;

-- Step 3: De-dupe Typed Table
-- ... assuming the user wanted dedupe
-- NOTE: Postgres doesn't "need" _airbyte_raw_id and could use ctid, but to match cloud DWs, that's what we use in this example
WITH cte AS (
	SELECT _airbyte_raw_id, row_number() OVER (
		PARTITION BY id ORDER BY _airbyte_read_at DESC
	) as row_number FROM public.users
)

DELETE FROM public.users
WHERE _airbyte_raw_id in (
	SELECT _airbyte_raw_id FROM cte WHERE row_number != 1
)
;

-- Step 4: Deal with CDC Deletion (and other special cases)
DELETE FROM public.users WHERE id IN (
	SELECT
		_airbyte_safe_cast_to_integer(_airbyte_data ->> 'id') as id -- based on the PK which we know from the connector catalog
	FROM z_airbyte.users_raw
	WHERE _airbyte_data ->> '_ab_cdc_deleted_at' IS NOT NULL
)
;

-- Step 5: Remove old entries from Raw table
DELETE FROM z_airbyte.users_raw
WHERE _airbyte_raw_id NOT IN (
	SELECT _airbyte_raw_id FROM public.users
)
;

-- Step 6: Apply typed_at timestamp where needed
UPDATE z_airbyte.users_raw
SET _airbyte_typed_at = NOW()
WHERE _airbyte_typed_at IS NULL
;


COMMIT;

----------------------------
--------- SYNC 3 -----------
----------------------------

-- Step 0: Prepare the raw table

CREATE SCHEMA IF NOT EXISTS z_airbyte;
CREATE TABLE IF NOT EXISTS z_airbyte.users_raw (
    "_airbyte_data" json NOT NULL, -- Airbyte column, cannot be null
    "_airbyte_raw_id" uuid NOT NULL, -- Airbyte column, cannot be null
    "_airbyte_read_at" timestamp NOT NULL, -- Airbyte column, cannot be null
    "_airbyte_typed_at" timestamp -- Airbyte column
);
CREATE INDEX IF NOT EXISTS "idx_users_raw__airbyte_raw_id" ON public.users USING BTREE ("_airbyte_raw_id");
CREATE INDEX IF NOT EXISTS "idx_users_raw__airbyte_read_at" ON public.users USING BTREE ("_airbyte_read_at");
CREATE INDEX IF NOT EXISTS "idx_users_raw__airbyte_typed_at" ON public.users USING BTREE ("_airbyte_typed_at");

-- Step 1: Load the raw data
-- Delete row 1 with CDC

INSERT INTO z_airbyte.users_raw ("_airbyte_data", "_airbyte_raw_id", "_airbyte_read_at") VALUES ('{   "id": 2,   "first_name": "Brian",   "age": 39,   "address": {     "city": "Menlo Park",     "zip": "99999"   }, "_ab_cdc_deleted_at": true}', gen_random_uuid(), NOW());

-- Moving the data and deduping happens in a transaction to prevent duplicates from appearing
BEGIN;

-- Step 2: Type the Data & handle errors
-- Note: We know the column names from the schema, so we don't need to anything refelxive to look up the column names

INSERT INTO public.users
SELECT
	_airbyte_safe_cast_to_integer(_airbyte_data ->> 'id') as id,
	_airbyte_safe_cast_to_text(_airbyte_data ->> 'first_name') as first_name,
	_airbyte_safe_cast_to_integer(_airbyte_data ->> 'age') as age,
	_airbyte_safe_cast_to_json(_airbyte_data ->> 'address') as address,
	CASE
		WHEN (_airbyte_data ->> 'id' IS NOT NULL) AND (_airbyte_safe_cast_to_integer(_airbyte_data ->> 'id') IS NULL) THEN '{"error": "Problem with `id`"}'
		WHEN (_airbyte_data ->> 'first_name' IS NOT NULL) AND (_airbyte_safe_cast_to_text(_airbyte_data ->> 'first_name') IS NULL) THEN '{"error": "Problem with `first_name`"}'
		WHEN (_airbyte_data ->> 'age' IS NOT NULL) AND (_airbyte_safe_cast_to_integer(_airbyte_data ->> 'age') IS NULL) THEN '{"error": "Problem with `age`"}'
		WHEN (_airbyte_data ->> 'address' IS NOT NULL) AND (_airbyte_safe_cast_to_json(_airbyte_data ->> 'address') IS NULL) THEN '{"error": "Problem with `address`"}'
		ELSE '{}'
	END::JSON as _airbyte_meta,
	_airbyte_raw_id,
	_airbyte_read_at
FROM z_airbyte.users_raw
WHERE
	_airbyte_typed_at IS NULL -- inserting only null values, we can recover from failed previous checkpoints
;

-- Step 3: De-dupe Typed Table
-- ... assuming the user wanted dedupe
-- NOTE: Postgres doesn't "need" _airbyte_raw_id and could use ctid, but to match cloud DWs, that's what we use in this example
WITH cte AS (
	SELECT _airbyte_raw_id, row_number() OVER (
		PARTITION BY id ORDER BY _airbyte_read_at DESC
	) as row_number FROM public.users
)

DELETE FROM public.users
WHERE _airbyte_raw_id in (
	SELECT _airbyte_raw_id FROM cte WHERE row_number != 1
)
;

-- Step 4: Deal with CDC Deletion (and other special cases)
DELETE FROM public.users WHERE id IN (
	SELECT
		_airbyte_safe_cast_to_integer(_airbyte_data ->> 'id') as id -- based on the PK which we know from the connector catalog
	FROM z_airbyte.users_raw
	WHERE _airbyte_data ->> '_ab_cdc_deleted_at' IS NOT NULL
)
;

-- Step 5: Remove old entries from Raw table
DELETE FROM z_airbyte.users_raw
WHERE _airbyte_raw_id NOT IN (
	SELECT _airbyte_raw_id FROM public.users
)
;

-- Step 6: Apply typed_at timestamp where needed
UPDATE z_airbyte.users_raw
SET _airbyte_typed_at = NOW()
WHERE _airbyte_typed_at IS NULL
;


COMMIT;
