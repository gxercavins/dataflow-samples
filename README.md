# Dataflow-samples

This repository contains some Google Cloud Dataflow / Apache Beam samples.

## Quickstart

Each folder contains specific instructions for the corresponding example.

## Examples

Currently, there are three examples available:

* **Adaptive triggers (Java)**: modify the behavior of triggers at the start and end of the same window so that you can have some degree of control on the output rate.
* **Dynamic destinations (Java)**: write dynamically to different BigQuery tables according to the schema of the processed record.
* **Normalize values (Python)**: normalize all PCollection values after calculating the maximum and minimum per each key.

## License

These examples are provided under the Apache License 2.0.

## Issues

Report any issue to the GitHub issue tracker.
