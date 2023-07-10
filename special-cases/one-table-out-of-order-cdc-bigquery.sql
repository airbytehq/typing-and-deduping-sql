-------------------------------------------------
--------- OUT OF ORDER DELETE 2 SYNCS -----------
-------------------------------------------------

CALL testing_evan_2052.reset();
CALL testing_evan_2052._airbyte_prepare_raw_table();

INSERT INTO testing_evan_2052.users_raw (`_airbyte_data`, `_airbyte_raw_id`, `_airbyte_extracted_at`) VALUES (JSON'{   "id": 1,    "updated_at": "2020-01-02T00:00:00Z",   "first_name": "USER DELETED",   "age": 38,   "address": {}, "_ab_cdc_deleted_at": true }', GENERATE_UUID(), CURRENT_TIMESTAMP());
CALL testing_evan_2052._airbyte_type_dedupe();

INSERT INTO testing_evan_2052.users_raw (`_airbyte_data`, `_airbyte_raw_id`, `_airbyte_extracted_at`) VALUES (JSON'{   "id": 1,    "updated_at": "2020-01-01T00:00:00Z",   "first_name": "USER UPDATED",   "age": 38,   "address": {} }', GENERATE_UUID(), CURRENT_TIMESTAMP());
CALL testing_evan_2052._airbyte_type_dedupe();

------------------------------------------------
--------- OUT OF ORDER DELETE 1 SYNC -----------
------------------------------------------------

CALL testing_evan_2052.reset();
CALL testing_evan_2052._airbyte_prepare_raw_table();

INSERT INTO testing_evan_2052.users_raw (`_airbyte_data`, `_airbyte_raw_id`, `_airbyte_extracted_at`) VALUES (JSON'{   "id": 1,    "updated_at": "2020-01-02T00:00:00Z",   "first_name": "USER DELETED",   "age": 38,   "address": {}, "_ab_cdc_deleted_at": true }', GENERATE_UUID(), CURRENT_TIMESTAMP());
INSERT INTO testing_evan_2052.users_raw (`_airbyte_data`, `_airbyte_raw_id`, `_airbyte_extracted_at`) VALUES (JSON'{   "id": 1,    "updated_at": "2020-01-01T00:00:00Z",   "first_name": "USER UPDATED",   "age": 38,   "address": {} }', GENERATE_UUID(), CURRENT_TIMESTAMP());
CALL testing_evan_2052._airbyte_type_dedupe();

--------------------------------------------------------------
--------- USER COMES BACK AFTER CDC DELETE 2 SYNCS -----------
--------------------------------------------------------------

CALL testing_evan_2052.reset();
CALL testing_evan_2052._airbyte_prepare_raw_table();

INSERT INTO testing_evan_2052.users_raw (`_airbyte_data`, `_airbyte_raw_id`, `_airbyte_extracted_at`) VALUES (JSON'{   "id": 1,    "updated_at": "2020-01-01T00:00:00Z",   "first_name": "USER DELETED",   "age": 38,   "address": {}, "_ab_cdc_deleted_at": true }', GENERATE_UUID(), CURRENT_TIMESTAMP());
CALL testing_evan_2052._airbyte_type_dedupe();

INSERT INTO testing_evan_2052.users_raw (`_airbyte_data`, `_airbyte_raw_id`, `_airbyte_extracted_at`) VALUES (JSON'{   "id": 1,    "updated_at": "2020-01-02T00:00:00Z",   "first_name": "USER COMES BACK",   "age": 38,   "address": {} }', GENERATE_UUID(), CURRENT_TIMESTAMP());
CALL testing_evan_2052._airbyte_type_dedupe();

-------------------------------------------------------------
--------- USER COMES BACK AFTER CDC DELETE 1 SYNC -----------
-------------------------------------------------------------

CALL testing_evan_2052.reset();
CALL testing_evan_2052._airbyte_prepare_raw_table();

INSERT INTO testing_evan_2052.users_raw (`_airbyte_data`, `_airbyte_raw_id`, `_airbyte_extracted_at`) VALUES (JSON'{   "id": 1,    "updated_at": "2020-01-01T00:00:00Z",   "first_name": "USER DELETED",   "age": 38,   "address": {}, "_ab_cdc_deleted_at": true }', GENERATE_UUID(), CURRENT_TIMESTAMP());
INSERT INTO testing_evan_2052.users_raw (`_airbyte_data`, `_airbyte_raw_id`, `_airbyte_extracted_at`) VALUES (JSON'{   "id": 1,    "updated_at": "2020-01-02T00:00:00Z",   "first_name": "USER COMES BACK",   "age": 38,   "address": {} }', GENERATE_UUID(), CURRENT_TIMESTAMP());
CALL testing_evan_2052._airbyte_type_dedupe();
