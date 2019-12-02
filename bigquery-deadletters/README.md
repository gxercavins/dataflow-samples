# BigQuery dead letters in Python

Dead letters to handle invalid inputs are a common Beam/Dataflow usage pattern. The built-in BigQuery connectors for both Java and Python SDKs expose that functionality but there are not many examples for the latter.

Here we'll present a Python example written as an answer to a [StackOverflow question](https://stackoverflow.com/questions/59102519/monitoring-writetobigquery/).

## Quickstart

Set up [authentication](https://cloud.google.com/docs/authentication/) your preferred way and install the Beam Python package (use of `virtualenv` is recommended): `pip install apache-beam[gcp]==2.16.0`.

The script can be tested locally with `python deadletters_direct.py` or using Dataflow with `python deadletters_dataflow.py`. Replace `PROJECT` and `BUCKET` env variables in the code as needed.

This code was tested with `apache-beam==2.16.0`.

## Example

Let's start by creating some dummy input data with 10 good lines and a bad row that does not conform to the table schema:

```python
schema = "index:INTEGER,event:STRING"

data = ['{0},good_line_{1}'.format(i + 1, i + 1) for i in range(10)]
data.append('this is a bad row')
```

Then, we will name the write result (`events` in this case) so that we can refer it later:

```python
events = (p
    | "Create data" >> beam.Create(data)
    | "CSV to dict" >> beam.ParDo(CsvToDictFn())
    | "Write to BigQuery" >> beam.io.gcp.bigquery.WriteToBigQuery(
        "{0}:dataflow_test.good_lines".format(PROJECT),
        schema=schema,
    )
 )
```

and then access the [`FAILED_ROWS`](https://github.com/apache/beam/blob/release-2.16.0/sdks/python/apache_beam/io/gcp/bigquery.py#L883) side output:

```python
(events[beam.io.gcp.bigquery.BigQueryWriteFn.FAILED_ROWS]
    | "Bad lines" >> beam.io.textio.WriteToText("error_log.txt"))
```

This works well with the `DirectRunner` and writes the good lines to BigQuery:

[![enter image description here][1]][1]

and logs the bad one to a local file:

```bash
$ cat error_log.txt-00000-of-00001 
('PROJECT_ID:dataflow_test.good_lines', {'index': 'this is a bad row'})
```

However, we'll need some additional flags to make it work with the `DataflowRunner`. We can encounter the `TypeError: 'PDone' object has no attribute '__getitem__'` error if we don't add `--experiments=use_beam_bq_sink` to use the new BigQuery sink (as of 2.16.0 SDK is not yet default).

If we get a `KeyError: 'FailedRows'` it's because the new sink will [default](https://github.com/apache/beam/blob/release-2.16.0/sdks/python/apache_beam/io/gcp/bigquery.py#L993) to load BigQuery jobs for batch pipelines:

> STREAMING_INSERTS, FILE_LOADS, or DEFAULT. An introduction on loading data to BigQuery: https://cloud.google.com/bigquery/docs/loading-data.
> DEFAULT will use STREAMING_INSERTS on Streaming pipelines and FILE_LOADS on Batch pipelines.

We can override this behavior by specifying `method='STREAMING_INSERTS'` in the `WriteToBigQuery` step and be able to run the job successfully:

[![enter image description here][2]][2]


  [1]: https://i.stack.imgur.com/yCcjy.png
  [2]: https://i.stack.imgur.com/qEEdb.png

## License

This example is provided under the Apache License 2.0.

## Issues

Report any issue to the GitHub issue tracker.
