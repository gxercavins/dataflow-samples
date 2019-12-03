# Dataflow-samples

This repository contains some Google Cloud Dataflow / Apache Beam samples.

## Quickstart

Each folder contains specific instructions for the corresponding example.

Use the below button to clone this repository into Cloud Shell and start right away:

[![Open this project in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.png)](https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/gxercavins/dataflow-samples&page=editor&tutorial=README.md)

## Examples

Currently, these are the examples available:

* **Adaptive triggers (Java)**: modify the behavior of triggers at the start and end of the same window so that you can have some degree of control on the output rate.
* **Assign sessions (Java)**: assign timestamped events into a given list of all possible sessions. 
* **Batch Schema auto-detect (Java)**: how to load multiple JSON files with disparate schemas into BigQuery.
* **BigQuery dead letters (Python)**: how to handle rows that could not be correctly streamed into BigQuery.
* **BigQuery Storage API (Java)**: how to read directly from a BigQuery table using the new Storage API.
* **Dynamic destinations (Java)**: write dynamically to different BigQuery tables according to the schema of the processed record.
* **Empty windows (Java)**: how to log/emit information even when the input source has no data for that window.
* **Filename match (Python)**: read from multiple files and prepend to each record the name of the matching file (optionally enrich with BigQuery).
* **Lag function (Python)**: how to compare an event with the equivalent one from the previous window.
* **Logging GroupByKey (Java)**: some ideas to log information about grouped elements using Stackdriver and BigQuery.
* **Normalize values (Python)**: normalize all PCollection values after calculating the maximum and minimum per each key.
* **Quick, Draw! dataset (Python)**: download raw data from a public dataset, convert to images and save them in `png` format.
* **RegEx pattern (Java)**: tag every path pattern and be able to associate each matched file with it.
* **Session windows (Python)**: example to demonstrate how to group events per user and session.
* **Timestamps in path (Java)**: process hourly files where timestamp needs to be inferred from folder structure.
* **Top10 distinct combiner (Python)**: we'll modify `TopCombineFn` to have unique keys when accumulating fired panes.
* **When are Pub/Sub messages ACKed? (Java)**: example to see what happens with `PubsubIO` in Dataflow.
* **With Timestamps (Java)**: assign processing time as element timestamp and shift to the past if needed.

In addition, the `UTILS` folder contains simple Dataflow snippets: adding labels, stopping jobs programmatically, process files selectively according to their format, understanding wall time, ensuring custom options are globally available, retrieving job ID or SDK version, writing BigQuery results in CSV format, enrich a PCollection with data from a BigQuery table, processing files using Pub/Sub notifications for GCS, etc.

The `PLAYGROUND` folder contains other more experimental examples that can be interesting to share such as trying to zip a PCollection, throttling a step or BeamSQL tests.

## License

These examples are provided under the Apache License 2.0.

## Issues

Report any issue to the GitHub issue tracker.
