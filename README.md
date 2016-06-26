# Analytics Cloud output plugin for Embulk

Embulk output plugin to load into Analytics Cloud.

## Overview

* **Plugin type**: output
* **Load all or nothing**: no
* **Resume supported**: no
* **Cleanup supported**: no

## Configuration

- **username**: analytics cloud username (string, required)
- **password**: analytics cloud password (string, required)
- **login_endpoint**: endpoint (string, default: `"https://login.salesforce.com"`)
- **edgemart_alias**: edgemart alias (string, required)
- **operation**: operation (string, default: `"Append"`)
- **metadata_json**: MetadataJson (string, default: `null`)
- **auto_metadata_settings**: auto creation for MetadataJson (object, default: `null`)
  - **connector**: connector name (string, default: `"EmbulkOutputPluginConnector"`)
  - **description**: description (string, default: `""`)
  - **precision**: precision for Numeric columns (integer, default: `18`)
  - **scale**: scale for Numeric columns (integer, default: `4`)
  - **default_value**: default value for Numeric columns (integer)
  - **format**: format for Timestamp columns (string, default: `"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"`)
- **batch_size**: data size for a InsightsExternalDataPart record (integer, default: `3000`)
- **version**: API version (string, default: `"34.0"`)

## Example

```yaml
out:
  type: analytics_cloud
  username: hoge
  password: fuga
  edgemart_alias: foobar
  auto_metadata_settings: {}
```


## Build

```
$ ./gradlew gem  # -t to watch change of files and rebuild continuously
```
