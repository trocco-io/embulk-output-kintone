# kintone output plugin for Embulk

[![Build Status](https://travis-ci.org/trocco-io/embulk-output-kintone.svg?branch=master)](https://travis-ci.org/trocco-io/embulk-output-kintone)

## Overview

kintone output plugin for Embulk stores app records from kintone.
embulk 0.9 is only supported due to the dependency of kintone-java-sdk 0.4.0, which requires java 8

## Configuration

- **domain**: kintone domain(FQDN) e.g. devfoo.cybozu.com (string, required)
- **username**: kintone username (string, optional)
- **password**: kintone password (string, optional)
- **token**: kintone app token. Username and password or token must be configured. If all of them are provided, this plugin uses username and password (string, optional)
- **app_id**: kintone app id (integer, required)
- **basic_auth_username**:  kintone basic auth username Please see kintone basic auth [here](https://jp.cybozu.help/general/en/admin/list_security/list_ip_basic/basic_auth.html) (string, optional)
- **basic_auth_password**:  kintone basic auth password (string, optional)
- **guest_space_id**: kintone app belongs to guest space, guest space id is required. (integer, optional)
- **mode**: kintone mode (string, required)
- **column_options** (required)
    - **field_code**: field code (string, required)
    - **type**: field type (string, required)
    - **timezone**: timezone to convert into `date` (string, default is `UTC`)
    - **update_key**: update key (boolean, default is `false`)

## Example

```yaml
out:
  type: kintone
    domain: example.cybozu.com
    username: username
    password: password
    app_id: 1
    mode: insert
    column_options:
      - {field_code: "id", type: "NUMBER"}
      - {field_code: "name", type: "SINGLE_LINE_TEXT"}
      - {field_code: "num", type: "NUMBER"}
      - {field_code: "date", type: "DATE"}
      - {field_code: "datetime", type: "DATETIME"}
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
