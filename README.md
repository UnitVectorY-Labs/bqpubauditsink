# bqpubauditsink

Ingests Pub/Sub audit JSON events and inserts the records into BigQuery.

## Overview

This application takes a stream of JSON messages from Pub/Sub produced by [firepubauditsource](https://github.com/UnitVectorY-Labs/firepubauditsource) and inserts each record into a BigQuery table.  The table structure here utilizes a common table pattern with the content being stored in JSON.

This application is designed to run in Cloud Run and is triggered by Pub/Sub messages using Eventarc.

## Configuration

This application is run as a docker container and requires the following environment variables to be set:

- `PROJECT_ID`: The GCP project ID where to write the records to BigQuery.
- `DATASET_NAME`: The dataset name in BigQuery where to write the records.
- `TABLE_NAME`: The table name in BigQuery where to write the records.

## Example Pub/Sub Message

The following are the examples of the input messages that this application processes.  The `value` and `oldValue` are input as part of the overall JSON object and are stored in JSON fields in BigQuery.  The `value` field being null will indicate a delete and set the `tombstone` field to true.

Inserting a Record:

```json
{
  "timestamp": "2024-10-27 12:00:00.000000",
  "database": "(default)",
  "documentPath": "mycollection/mydoc",
  "value": {
    "foo": "new"
  },
  "oldValue": null
}
```

Updating a Record:

```json
{
  "timestamp": "2024-10-27 12:00:10.000000",
  "database": "(default)",
  "documentPath": "mycollection/mydoc",
  "value": {
    "foo": "updated"
  },
  "oldValue": {
    "foo": "bar"
  }
}
```

Deleting a Record:

```json
{
  "timestamp": "2024-10-27 12:00:20.000000",
  "database": "(default)",
  "documentPath": "mycollection/mydoc",
  "value": null,
  "oldValue": {
    "foo": "bar"
  }
}
```

## Create Database Table

The design of this allows for a single BigQuery table to handle multiple multiple databases and collections and is not dependent on the schema of the JSON data.  The table is clustered by `database`, `documentPath`, and `timestamp` to optimize the nominal queries for looking for most recent records and pruning old records.

```sql
CREATE TABLE `project.dataset.table`
(
  documentPath STRING NOT NULL,
  timestamp TIMESTAMP NOT NULL,
  database STRING NOT NULL,
  value JSON,
  oldValue JSON,
  tombstone BOOL
)
CLUSTER BY database, documentPath, timestamp;
```

## Prune Query

This query will remove all records that are not the most recent for a given `database` and `documentPath`.  This is useful for keeping the table size down and only keeping the most recent records. The intent is to run this query on a periodic basis to keep the table size down depending on the velocity of data changes.

```sql
DELETE FROM `project.dataset.table` AS m
WHERE m.timestamp < (
    SELECT MAX(sub.timestamp)
    FROM `project.dataset.table` AS sub
    WHERE m.database = sub.database AND m.documentPath = sub.documentPath
);
```

## Most Recent Query

This query will return the most recent records for each `database` and `documentPath`. This allows for querying the data in BigQuery in a way that reflects the most recent state of the source database. The `value` JSON field can be extracted into separate columns as needed.  Additionally a view can be created using  this query to make it easier to query the most recent records.

```sql
SELECT 
  m.database,
  m.documentPath,
  m.timestamp,
  JSON_EXTRACT_SCALAR(m.value, "$.foo") AS foo
FROM 
  `project.dataset.table` m
JOIN (
    SELECT 
      database,
      documentPath, 
      MAX(timestamp) AS latest_update
    FROM 
      `project.dataset.table`
    GROUP BY 
      database, documentPath
) AS latest_records
ON 
  m.database = latest_records.database 
  AND m.documentPath = latest_records.documentPath 
  AND m.timestamp = latest_records.latest_update
WHERE 
  m.tombstone = FALSE;
```

## Limitations

- The `value` and `oldValue` records being stored as JSON fields is a tradeoff between flexibility and performance.  This allows for the schema to be flexible and not require changes to the BigQuery table schema when the JSON structure changes.  However, this can make querying the data more expensive as all data must be queried and the columnar support of BigQuery is not utilized.
- The `tombstone` field indicates deleted records, but the final deleted record will remain in the table.
