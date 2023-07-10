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

DROP TABLE IF EXISTS public.users;
DROP TABLE IF EXISTS z_airbyte.users_raw;

CREATE TABLE public.users (
    "_airbyte_raw_id" uuid NOT NULL -- Airbyte column, cannot be null
    , "_airbyte_meta" json NOT NULL -- Airbyte column, cannot be null
    , "_airbyte_extracted_at" timestamp NOT NULL -- Airbyte column, cannot be null
    , "id" int8 -- PK cannot be null, but after raw insert and before typing, row will be null
    , "first_name" text
    , "age" int8
    , "address" json
    , "updated_at" timestamp
);

-- indexes for colums we will use
CREATE INDEX "idx_users__airbyte_extracted_at" ON public.users USING BTREE ("_airbyte_extracted_at");
CREATE INDEX "idx_users__airbyte_raw_id" ON public.users USING BTREE ("_airbyte_raw_id");
CREATE INDEX "idx_users_pk" ON public.users USING BTREE ("id");
CREATE INDEX "idx_updated_at_pk" ON public.users USING BTREE ("updated_at");


---------------------------------------
--------- SAFE CAST METHODS -----------
---------------------------------------

-- Some DBs (BQ) have this built in, but we can do more-or-less the same thing with custom functions

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

CREATE OR REPLACE FUNCTION _airbyte_safe_cast_to_timestamp(v_input text)
RETURNS TIMESTAMP AS $$
DECLARE v_ts_value TIMESTAMP DEFAULT NULL;
BEGIN
    BEGIN
        v_ts_value := v_input::TIMESTAMP;
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Invalid timestamp value: "%".  Returning NULL.', v_input;
        RETURN NULL;
    END;
RETURN v_ts_value;
END;
$$ LANGUAGE plpgsql;

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

-------------------------------------
--------- TYPE AND DEDUPE -----------
-------------------------------------

CREATE OR REPLACE FUNCTION _airbyte_prepare_raw_table()
RETURNS TEXT AS $$

BEGIN
  CREATE SCHEMA IF NOT EXISTS z_airbyte;
  CREATE TABLE IF NOT EXISTS z_airbyte.users_raw (
      "_airbyte_raw_id" uuid NOT NULL, -- Airbyte column, cannot be null
      "_airbyte_data" json NOT NULL, -- Airbyte column, cannot be null
      "_airbyte_extracted_at" timestamp NOT NULL, -- Airbyte column, cannot be null
      "_airbyte_loaded_at" timestamp, -- Airbyte column
      PRIMARY KEY ("_airbyte_raw_id")
  );
  CREATE INDEX IF NOT EXISTS "idx_users_raw__airbyte_raw_id" ON z_airbyte.users_raw USING BTREE ("_airbyte_raw_id");
  CREATE INDEX IF NOT EXISTS "idx_users_raw__airbyte_extracted_at" ON z_airbyte.users_raw USING BTREE ("_airbyte_extracted_at");
  CREATE INDEX IF NOT EXISTS "idx_users_raw__airbyte_loaded_at" ON z_airbyte.users_raw USING BTREE ("_airbyte_loaded_at");
  RETURN 'SUCCESS';
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION _airbyte_type_dedupe()
RETURNS TEXT AS $$

DECLARE missing_pk_count INTEGER DEFAULT 0;

BEGIN

  -- Step 1: Validate the incoming data
  -- We can't really do this properly in the pure-SQL example here, but we should throw if any row doesn't have a PK
  missing_pk_count := (
    SELECT COUNT(1)
    FROM Z_AIRBYTE.USERS_RAW
    WHERE
      "_airbyte_loaded_at" IS NULL
      AND _airbyte_safe_cast_to_integer(_airbyte_data ->> 'id') IS NULL
    );

  IF missing_pk_count > 0 THEN
    RAISE EXCEPTION 'Table % has % rows missing a primary key', raw_table, missing_pk_count;
  END IF;

  -- Moving the data and deduping happens in a transaction to prevent duplicates from appearing
  -- BEGIN

  -- Step 2: Move the new data to the typed table
  WITH intermediate_data AS (
    SELECT
      _airbyte_safe_cast_to_integer(_airbyte_data ->> 'id') as id,
      _airbyte_safe_cast_to_text(_airbyte_data ->> 'first_name') as first_name,
      _airbyte_safe_cast_to_integer(_airbyte_data ->> 'age') as age,
      _airbyte_safe_cast_to_json(_airbyte_data ->> 'address') as address,
      _airbyte_safe_cast_to_timestamp(_airbyte_data ->> 'updated_at') as updated_at,
      (
        CASE WHEN (_airbyte_data ->> 'id' IS NOT NULL) AND (_airbyte_safe_cast_to_integer(_airbyte_data ->> 'id') IS NULL) THEN ARRAY['Problem with `id`'] ELSE ARRAY[]::text[] END
        ||
        CASE WHEN (_airbyte_data ->> 'first_name' IS NOT NULL) AND (_airbyte_safe_cast_to_text(_airbyte_data ->> 'first_name') IS NULL) THEN ARRAY['Problem with `first_name`'] ELSE ARRAY[]::text[] END
        ||
        CASE WHEN (_airbyte_data ->> 'age' IS NOT NULL) AND (_airbyte_safe_cast_to_integer(_airbyte_data ->> 'age') IS NULL) THEN ARRAY['Problem with `age`'] ELSE ARRAY[]::text[] END
        ||
        CASE WHEN (_airbyte_data ->> 'updated_at' IS NOT NULL) AND (_airbyte_safe_cast_to_timestamp(_airbyte_data ->> 'updated_at') IS NULL) THEN ARRAY['Problem with `updated_at`'] ELSE ARRAY[]::text[] END
        ||
        CASE WHEN (_airbyte_data ->> 'address' IS NOT NULL) AND (_airbyte_safe_cast_to_json(_airbyte_data ->> 'address') IS NULL) THEN ARRAY['Problem with `address`'] ELSE ARRAY[]::text[] END
      ) as _airbyte_cast_errors
      , _airbyte_raw_id
      , _airbyte_extracted_at
    FROM z_airbyte.users_raw
    WHERE
      _airbyte_loaded_at IS NULL -- inserting only new/null values, we can recover from failed previous checkpoints
      OR (
        -- Temporarily place back an entry for any CDC-deleted record so we can order them properly by cursor.  We only need the PK and cursor value
        _airbyte_loaded_at IS NOT NULL
        AND _airbyte_data ->> '$._ab_cdc_deleted_at' IS NOT NULL
      )
  )

  INSERT INTO public.users
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
  SELECT
      id,
      first_name,
      age,
      updated_at,
      address,
      CASE
        WHEN array_length(_airbyte_cast_errors, 1) = 0 THEN '{"errors": []}'::JSON
        ELSE json_build_object('errors', _airbyte_cast_errors)
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

  WITH cte AS (
    SELECT _airbyte_raw_id, row_number() OVER (
      PARTITION BY id ORDER BY updated_at DESC, _airbyte_extracted_at DESC
    ) as row_number FROM public.users
  )

  DELETE FROM public.users
  WHERE
    -- Delete any rows which are not the most recent for a given PK
    _airbyte_raw_id IN (
      SELECT _airbyte_raw_id FROM cte WHERE row_number != 1
    )
  ;

  -- Step 4: Remove old entries from Raw table
  DELETE FROM
    z_airbyte.users_raw
  WHERE
    _airbyte_raw_id NOT IN (
      SELECT _airbyte_raw_id FROM public.users
    )
  ;

  -- Step 5: Clean out CDC deletes from final table
  -- Only run this step if _ab_cdc_deleted_at is a property of the stream
  /*
  DELETE FROM testing_evan_2052.users
  WHERE _ab_cdc_deleted_at IS NOT NULL
  */

  -- the following will always work, even if there is no _ab_cdc_deleted_at column, but it is slower
  DELETE FROM public.users
  WHERE
    -- Delete rows that have been CDC deleted
    id IN (
      SELECT
        _airbyte_safe_cast_to_integer(_airbyte_data ->> 'id') as id -- based on the PK which we know from the connector catalog
      FROM z_airbyte.users_raw
      WHERE _airbyte_data ->> '_ab_cdc_deleted_at' IS NOT NULL
    )
  ;

  -- Step 6: Apply typed_at timestamp where needed
  UPDATE z_airbyte.users_raw
  SET _airbyte_loaded_at = NOW()
  WHERE _airbyte_loaded_at IS NULL
  ;


  RETURN 'SUCCESS';
END;
$$ LANGUAGE plpgsql;

----------------------------
--------- SYNC 1 -----------
----------------------------

SELECT _airbyte_prepare_raw_table();

-- Load the raw data

INSERT INTO z_airbyte.users_raw ("_airbyte_data", "_airbyte_raw_id", "_airbyte_extracted_at") VALUES ('{   "id": 1,    "updated_at": "2020-01-01T00:00:00Z",   "first_name": "Evan",   "age": 38,   "address": {     "city": "San Francisco",     "zip": "94001"   } }', gen_random_uuid(), NOW());
INSERT INTO z_airbyte.users_raw ("_airbyte_data", "_airbyte_raw_id", "_airbyte_extracted_at") VALUES ('{   "id": 2,    "updated_at": "2020-01-01T00:00:01Z",   "first_name": "Brian",   "age": 39,   "address": {     "city": "Menlo Park",     "zip": "94002"   } }', gen_random_uuid(), NOW());
INSERT INTO z_airbyte.users_raw ("_airbyte_data", "_airbyte_raw_id", "_airbyte_extracted_at") VALUES ('{   "id": 3,    "updated_at": "2020-01-01T00:00:02Z",   "first_name": "Edward",   "age": 40,   "address": {     "city": "Sunyvale",     "zip": "94003"   } }', gen_random_uuid(), NOW());
INSERT INTO z_airbyte.users_raw ("_airbyte_data", "_airbyte_raw_id", "_airbyte_extracted_at") VALUES ('{   "id": 4,    "updated_at": "2020-01-01T00:00:03Z",   "first_name": "Joe",   "address": {     "city": "Seattle",     "zip": "98999"   } }', gen_random_uuid(), NOW()); -- Joe is missing an age, null OK

SELECT _airbyte_type_dedupe();

----------------------------
--------- SYNC 2 -----------
----------------------------

SELECT _airbyte_prepare_raw_table();

-- Load the raw data
-- Age update for Evan (user 1)
-- There is an update for Brian (user 2, new address.zip)
-- There is an update for Edward (user 3, age is invalid)
-- No update for Joe (user 4)

INSERT INTO z_airbyte.users_raw ("_airbyte_data", "_airbyte_raw_id", "_airbyte_extracted_at") VALUES ('{   "id": 1,    "updated_at": "2020-01-02T00:00:00Z",   "first_name": "Evan",   "age": 39,   "address": {     "city": "San Francisco",     "zip": "94001"   } }', gen_random_uuid(), NOW());
INSERT INTO z_airbyte.users_raw ("_airbyte_data", "_airbyte_raw_id", "_airbyte_extracted_at") VALUES ('{   "id": 2,    "updated_at": "2020-01-02T00:00:01Z",   "first_name": "Brian",   "age": 39,   "address": {     "city": "Menlo Park",     "zip": "99999"   } }', gen_random_uuid(), NOW());
INSERT INTO z_airbyte.users_raw ("_airbyte_data", "_airbyte_raw_id", "_airbyte_extracted_at") VALUES ('{   "id": 3,    "updated_at": "2020-01-02T00:00:02Z",   "first_name": "Edward",   "age": "forty",   "address": {     "city": "Sunyvale",     "zip": "94003"   } }', gen_random_uuid(), NOW());

SELECT _airbyte_type_dedupe();

----------------------------
--------- SYNC 3 -----------
----------------------------

SELECT _airbyte_prepare_raw_table();

-- Load the raw data
-- Delete row 1 with CDC
-- Insert multiple records for a new user (with age incrementing each time)

INSERT INTO z_airbyte.users_raw ("_airbyte_data", "_airbyte_raw_id", "_airbyte_extracted_at") VALUES ('{   "id": 2,    "updated_at": "2020-01-03T00:00:00Z",   "first_name": "Brian",   "age": 39,   "address": {     "city": "Menlo Park",     "zip": "99999"   }, "_ab_cdc_deleted_at": true}', gen_random_uuid(), NOW());
INSERT INTO z_airbyte.users_raw ("_airbyte_data", "_airbyte_raw_id", "_airbyte_extracted_at") VALUES ('{   "id": 5,    "updated_at": "2020-01-03T00:00:01Z",   "first_name": "Cynthia",   "age": 40,   "address": {     "city": "Redwood City",     "zip": "98765"   }}', gen_random_uuid(), NOW());
INSERT INTO z_airbyte.users_raw ("_airbyte_data", "_airbyte_raw_id", "_airbyte_extracted_at") VALUES ('{   "id": 5,    "updated_at": "2020-01-03T00:00:02Z",   "first_name": "Cynthia",   "age": 41,   "address": {     "city": "Redwood City",     "zip": "98765"   }}', gen_random_uuid(), NOW());
INSERT INTO z_airbyte.users_raw ("_airbyte_data", "_airbyte_raw_id", "_airbyte_extracted_at") VALUES ('{   "id": 5,    "updated_at": "2020-01-03T00:00:03Z",   "first_name": "Cynthia",   "age": 42,   "address": {     "city": "Redwood City",     "zip": "98765"   }}', gen_random_uuid(), NOW());

SELECT _airbyte_type_dedupe();

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

SELECT NOW();
