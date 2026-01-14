# kintone output plugin for Embulk

[![Build Status](https://travis-ci.org/trocco-io/embulk-output-kintone.svg?branch=master)](https://travis-ci.org/trocco-io/embulk-output-kintone)

## Overview

kintone output plugin for Embulk stores app records from kintone.

## Configuration

- **domain**: kintone domain (FQDN) e.g. devfoo.cybozu.com (string, required)
- **username**: kintone username (string, optional)
- **password**: kintone password (string, optional)
- **token**: kintone app token. Username and password or token must be configured. If all of them are provided, this plugin uses username and password (string, optional)
- **app_id**: kintone app id (integer, required)
- **basic_auth_username**: kintone basic auth username Please see kintone basic auth [here](https://jp.cybozu.help/general/en/admin/list_security/list_ip_basic/basic_auth.html) (string, optional)
- **basic_auth_password**: kintone basic auth password (string, optional)
- **guest_space_id**: kintone app belongs to guest space, guest space id is required. (integer, optional)
- **mode**: kintone mode (string, required)
- **update_key**: Column name to set update key (string, required if mode is update or upsert)
- **reduce_key**: Key column name to reduce expanded SUBTABLE (string, optional)
- **sort_columns**: List of columns for sorting input records (array of objects, optional)
    - **name**: Column name (string, required)
    - **order**: Sort order (string `asc` or `desc`, required)
- **max_sort_tmp_files**: Maximum number of temporary files for sorting input records (integer, default is `1024`)
- **max_sort_memory**: Maximum memory usage for sorting input records (bytes in long, default is the estimated available memory, which is the approximate value of the JVM's current free memory)
- **prefer_nulls**: Whether to set fields to null instead of default value of type when column is null (boolean, default is `false`)
- **ignore_nulls**: Whether to completely ignore fields when column is null (boolean, default is `false`)
- **skip_if_non_existing_id_or_update_key**: The skip policy if the record corresponding to the id or update key does not exist (string `auto`, `never` or `always`, default is `auto`). No effect for insert mode.
    - **auto**:
        - **update mode**: Skip the record if no id or update key value is specified.
        - **upsert mode**: Skip the record if corresponds to the id does not exist or no update key value is specified.
    - **never**: Never skip the record even if corresponds to the id or update key does not exist.
        - **update mode**: Throw exception if no id or update key value is specified.
        - **upsert mode**: Insert the record if corresponds to the id or update key does not exist (also, if no id or update key value is specified).
    - **always**: Always skip the record if corresponds to the id or update key does not exist (also, if no id or update key value is specified). update mode and upsert mode will the same behavior (only updated, never inserted).
- **column_options** advanced: a key-value pairs where key is a column name and value is options for the column.
    - **field_code**: field code (string, required)
    - **type**: field type (string, required). See [this page](https://cybozu.dev/ja/kintone/docs/overview/field-types/#field-type-update) for list of available types. However, following types are not yet supported
        - `USER_SELECT`, `ORGANIZATION_SELECT`, `GROUP_SELECT`, `FILE`
    - **timezone**: timezone to convert into `date` (string, default is `UTC`)
    - **val_sep**: Used to specify multiple checkbox values (string, default is `,`)
    - **sort_columns**: List of columns for sorting rows in SUBTABLE. Available only if type is `SUBTABLE` (array of objects, optional)
        - **name**: Column name (string, required)
        - **order**: Sort order (string `asc` or `desc`, required)
- **chunk_size**: Maximum number of records to request at once (integer, default is `100`)
- **error_records_detail_output_file**: Output file path for detailed error records in JSONL format. When Kintone API errors occur, failed records are logged with their data, error codes, and error messages. Each line contains a JSON object with `record_data`, `error_code`, and `error_message` fields (string, optional)

## Example

```yaml
out:
  type: kintone
  domain: example.cybozu.com
  username: username
  password: password
  app_id: 1
  mode: upsert
  update_key: id
  error_records_detail_output_file: /tmp/kintone_error_records.jsonl
  column_options:
    id: {field_code: "id", type: "NUMBER"}
    name: {field_code: "name", type: "SINGLE_LINE_TEXT"}
    number: {field_code: "num", type: "NUMBER"}
    date: {field_code: "date", type: "DATE"}
    date_time: {field_code: "datetime", type: "DATETIME"}
```

### For reduce expanded SUBTABLE

```yaml
out:
  ...
  reduce_key: id
  column_options:
    id: {field_code: "id", type: "NUMBER"}
    ...
    table: {field_code: "table", type: "SUBTABLE", sort_columns: [{name: number, order: asc}, {name: text, order: desc}]}
    table.number: {field_code: "number_in_table", type: "NUMBER"}
    table.text: {field_code: "text_in_table", type: "SINGLE_LINE_TEXT"}
```

#### KINTONE

| Form                      | Field Code      |
| ------------------------- | --------------- |
| Table's own               | table           |
| NUMBER In Table           | number_in_table |
| SINGLE_LINE_TEXT In Table | text_in_table   |

#### INPUT CSV

```csv
id,table.number,table.text
1,0,test0
1,1,test1
2,0,test0
```

#### RESULT

ID:1

| NUMBER        | SINGLE_LINE_TEXT |
| ------------- | ---------------- |
| 0             | test0            |
| 1             | test1            |

ID:2

| NUMBER        | SINGLE_LINE_TEXT |
| ------------- | ---------------- |
| 0             | test0            |

## Error Records Detail Output

When `error_records_detail_output_file` is configured, failed records are logged in JSONL format. Each line contains a JSON object with the following structure:

```json
{"record_data":{"id":"123","name":"John Doe"},"error_code":"GAIA_RE01","error_message":"Field validation error: name: This field is required"}
```

### Fields

- **record_data**: Original record data that failed to be processed
- **error_code**: Kintone API error code (e.g., GAIA_RE01, GAIA_RE18)  
- **error_message**: Detailed error message including field-specific errors

### Example Error Log File

```
{"record_data":{"id":"001","email":"invalid-email"},"error_code":"GAIA_RE01","error_message":"Field validation error: email: Invalid email format"}
{"record_data":{"id":"002","required_field":null},"error_code":"GAIA_RE01","error_message":"Field validation error: required_field: This field is required"}
```

## Build

```
$ ./gradlew gem  # -t to watch change of files and rebuild continuously
```

## Development
```
$ ./gradlew build
$ ./gradlew test
```
