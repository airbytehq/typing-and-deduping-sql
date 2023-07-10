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
