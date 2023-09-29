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
  column_options:
    id: {field_code: "id", type: "NUMBER"}
    name: {field_code: "name", type: "SINGLE_LINE_TEXT"}
    number: {field_code: "num", type: "NUMBER"}
    date: {field_code: "date", type: "DATE"}
    date_time: {field_code: "datetime", type: "DATETIME"}
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
