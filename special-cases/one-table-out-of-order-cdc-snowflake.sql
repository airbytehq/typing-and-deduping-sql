----------------------------
--------- BATCH 1 ----------
----------------------------

CALL PUBLIC._AIRBYTE_PREPARE_RAW_TABLE();

INSERT INTO Z_AIRBYTE.USERS_RAW ("_airbyte_data", "_airbyte_raw_id", "_airbyte_extracted_at") SELECT PARSE_JSON($${
  id: 1,
  first_name: "CREATE JOE",
  age: 35,
  updated_at: "2020-01-01T00:00:01Z",
  address:{
    city: "Seattle",
    zip: "99999"
} }$$), UUID_STRING(), CURRENT_TIMESTAMP();

CALL PUBLIC._AIRBYTE_TYPE_DEDUPE();

----------------------------
--------- BATCH 2 ----------
----------------------------

CALL PUBLIC._AIRBYTE_PREPARE_RAW_TABLE();

INSERT INTO Z_AIRBYTE.USERS_RAW ("_airbyte_data", "_airbyte_raw_id", "_airbyte_extracted_at") SELECT PARSE_JSON($${
  _ab_cdc_deleted_at: true,
  id: 1,
  first_name: "DELETE JOE",
  age: 35,
  updated_at: "2020-01-01T00:00:03Z",
  address:{
    city: "Seattle",
    zip: "99999"
} }$$), UUID_STRING(), CURRENT_TIMESTAMP();

CALL PUBLIC._AIRBYTE_TYPE_DEDUPE();

----------------------------
--------- BATCH 3 ----------
----------------------------

CALL PUBLIC._AIRBYTE_PREPARE_RAW_TABLE();

INSERT INTO Z_AIRBYTE.USERS_RAW ("_airbyte_data", "_airbyte_raw_id", "_airbyte_extracted_at") SELECT PARSE_JSON($${
  id: 1,
  first_name: "UPDATE JOE",
  age: 35,
  updated_at: "2020-01-01T00:00:02Z",
  address:{
    city: "Seattle",
    zip: "99999"
} }$$), UUID_STRING(), CURRENT_TIMESTAMP();


CALL PUBLIC._AIRBYTE_TYPE_DEDUPE();

----------------------
-- FINAL VALIDATION --
----------------------
/*

Joe should not exist in the final table, but in the raw table the most recent record (by cursor) should remain, the DELETE JOE entry.
The "UPDATE JOE" entry should not be present in either the final or raw tables

*/

SELECT CURRENT_TIMESTAMP();
