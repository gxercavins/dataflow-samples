# BigQuery results to GCS in CSV format

This example shows how to get results from a BigQuery public table, parse them to CSV and write the files to GCS. Originally written as an answer to a StackOverflow [question](https://stackoverflow.com/questions/52929387/write-bigquery-results-to-gcs-in-csv-format-using-apache-beam/).

## Quickstart

* Set up [authentication](https://cloud.google.com/docs/authentication/) your preferred way
* Install the `apache-beam[gcp]` Python package: `apache-beam[gcp]` (use of `virtualenv` is recommended)
* Execute `./run.sh PROJECT_ID BUCKET_NAME`
* Set the `$PROJECT`, `$BUCKET` and stage the Dataflow template:

This code was tested locally with `apache-beam[gcp]==2.8.0`.

## Example

For this example I used the William Shakespeare public [table][2] and the following query that returns the 10 most used words (with 4 or more characters to quickly filter out some stopwords):

> SELECT word, word_count, corpus FROM \`bigquery-public-data.samples.shakespeare\` WHERE CHAR_LENGTH(word) > 3 ORDER BY word_count DESC LIMIT 10

We read the query results from within the pipeline with:

```python
BQ_DATA = p | 'read_bq_view' >> beam.io.Read(
    beam.io.BigQuerySource(query=query, use_standard_sql=True))
```

Where each `BQ_DATA` record now contains key-value pairs:

```python
{u'corpus': u'hamlet', u'word': u'HAMLET', u'word_count': 407}
{u'corpus': u'kingrichardiii', u'word': u'that', u'word_count': 319}
{u'corpus': u'othello', u'word': u'OTHELLO', u'word_count': 313}
...
```

As we don't want to write the field name each time (we'll just use headers instead) we will apply a `beam.Map` function to yield only values:

```python
BQ_VALUES = BQ_DATA | 'read values' >> beam.Map(lambda x: x.values())
```

An excerpt of `BQ_VALUES` which does no longer contain the repeated `corpus`, `word` and `word_count` field names:
```python
[u'hamlet', u'HAMLET', 407]
[u'kingrichardiii', u'that', 319]
[u'othello', u'OTHELLO', 313]
...
```

And finally we map again to convert the previous list to comma separated value rows (take into account that you would need to escape double quotes if they can appear within a field):

```python
BQ_CSV = BQ_VALUES | 'CSV format' >> beam.Map(
    lambda row: ', '.join(['"'+ str(column) +'"' for column in row]))
```
Now we write the results to GCS using [`WriteToText`][1] to add a `.csv` suffix and `headers`:

```python
BQ_CSV | 'Write_to_GCS' >> beam.io.WriteToText(
    'gs://{0}/results/output'.format(BUCKET), file_name_suffix='.csv', header='word, word count, corpus')
```

And we can inspect the results written to the GCS bucket:

```bash
$ gsutil cat gs://$BUCKET/results/output-00000-of-00001.csv
word, word count, corpus
"hamlet", "HAMLET", "407"
"kingrichardiii", "that", "319"
"othello", "OTHELLO", "313"
"merrywivesofwindsor", "MISTRESS", "310"
"othello", "IAGO", "299"
"antonyandcleopatra", "ANTONY", "284"
"asyoulikeit", "that", "281"
"antonyandcleopatra", "CLEOPATRA", "274"
"measureforemeasure", "your", "274"
"romeoandjuliet", "that", "270"
```

  [1]: https://beam.apache.org/releases/pydoc/2.6.0/apache_beam.io.textio.html#apache_beam.io.textio.WriteToText
  [2]: https://bigquery.cloud.google.com/table/bigquery-public-data:samples.shakespeare?tab=preview

## License

These examples are provided under the Apache License 2.0.

## Issues

Report any issue to the GitHub issue tracker.
