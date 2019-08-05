# Utils

This folder contains some quick snippets.

## Quickstart

Each folder contains specific instructions for the corresponding example.

## Examples

* **Add Labels (Java)**: how to add labels (in code) to the job in order to track expenses in the Billing Export.
* **BigQuery Null Values (Java)**: how to write records with `Null` values to BigQuery using Dataflow.
* **BigQuery Results to CSV (Python)**: how to write BigQuery results to GCS in CSV format.
* **Dynamic BigQuery Writes (Java + Python)**: how to write to different BigQuery tables according to data.
* **Enrich with BigQuery (Java)**: how to read BigQuery data within an intermediate step to enrich an existing PCollection.
* **Escaping commas (Bash)**: how to pass a comma as a parameter when invoking a template.
* **Global Options (Java + Python)**: access template custom options globally.
* **Input Filenames (Java)**: how to process files according to their format (i.e. CSV or XML).
* **Longest Row (Python)**: how to find the record with more words in a file using the `Top` transform.
* **Map Elements (Java)**: how to apply the `MapElements` transform.
* **Map vs ParDo (Python)**: how to call the same function with `Beam.Map` and `Beam.ParDo`.
* **Multiple Topics (Java)**: pass as input option a comma-separated list of Pub/Sub topics.
* **One Row, One File (Java)**: write each row/record to a different output file.
* **One Window, One File (Python)**: write elements from each window to a different output file.
* **Pub/Sub publish time (Java)**: how to get message publish time in Dataflow.
* **Pub/Sub to BigQuery Template (Java)**: adapt the official template to use an input subscription instead of topic.
* **Pub/Sub and Windowing (Scala)**: testing streaming pipelines with Spotify's Scio.
* **ReadAllFromText templated (Python)**: how to use template parameters with ReadAllFromText.
* **Retrieve Job ID (Python)**: how to retrieve the Job ID from within the job pipeline.
* **SHA-256 (Java)**: how to calculate SHA-256 Hash of each file.
* **Stop Job (Python)**: how to programmatically stop a Dataflow job using the Python Google API Client Library.
* **Understanding Wall Time (Java)**: quick idea to visualize wall time.
* **Which SDK? (Python)**: how to retrieve the list of jobs with specific SDK versions.

## License

These examples are provided under the Apache License 2.0.

## Issues

Report any issue to the GitHub issue tracker.
