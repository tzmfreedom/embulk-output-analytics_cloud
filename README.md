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
- **login_endpoint**: endpoint (string, default: `https://login.salesforce.com`)
- **edgemart_alias**: edgemart alias (string, required)
- **operation**: operation (string, default: `"Append"`)
- **metadata_json**: MetadataJson (string, default: `null`)
- **auto_metadata**: auto creation for MetadataJson (integer, required)
- **batch_size**: data size for a InsightsExternalDataPart record (integer, default: `"3000"`)
- **version**: API version (string, default: `"34.0"`)

## Example

```yaml
out:
  type: analytics_cloud
  username: hoge
  password: fuga
  edgemart_alias: foobar
```


## Build

```
$ ./gradlew gem  # -t to watch change of files and rebuild continuously
```
