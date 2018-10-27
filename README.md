# Dataflow-samples

This repository contains some Google Cloud Dataflow / Apache Beam samples.

## Quickstart

Each folder contains specific instructions for the corresponding example.

## Examples

Currently, there are three examples available:

* **Adaptive triggers (Java)**: modify the behavior of triggers at the start and end of the same window so that you can have some degree of control on the output rate.
* **Dynamic destinations (Java)**: write dynamically to different BigQuery tables according to the schema of the processed record.
* **Logging GroupByKey (Java)**: some ideas to log information about grouped elements using Stackdriver and BigQuery.
* **Normalize values (Python)**: normalize all PCollection values after calculating the maximum and minimum per each key.
* **Quick, Draw! dataset (Python)**: download raw data from a public dataset, convert to images and save them in `png` format.
* **When are Pub/Sub messages ACKed? (Java)**: example to see what happens with `PubsubIO` in Dataflow.
* **With Timestamps (Java)**: assign processing time as element timestamp and shift to the past if needed.

In addition, the `utils` folder contains simple Dataflow snippets: adding labels, stopping jobs programmatically, process files selectively according to their format, understanding wall time, ensuring custom options are globally available, retrieving job ID or SDK version, writing BigQuery results in CSV format, etc.

## License

These examples are provided under the Apache License 2.0.

## Issues

Report any issue to the GitHub issue tracker.
