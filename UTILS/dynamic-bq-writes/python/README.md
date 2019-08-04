# Dynamic BigQuery destinations in Python

Now, as per this [StackOverflow answer](https://stackoverflow.com/a/56370262/6121516), Python SDK batch jobs supports writing to different BiQuery destinations by passing a function as `table` in the [new BigQuery sink](https://beam.apache.org/releases/pydoc/current/apache_beam.io.gcp.bigquery.html#module-apache_beam.io.gcp.bigquery).
For a complete Java example please refer [here](https://github.com/gxercavins/dataflow-samples/tree/master/dynamic-destinations).

## Quickstart

Set up [authentication](https://cloud.google.com/docs/authentication/) your preferred way and install the Beam Python package (use of `virtualenv` is recommended): `pip install apache-beam[gcp]>=2.12.0`. Take into account that you'll need to use 2.12.0 or higher for this feature.

The script can be tested locally with the `DirectRunner`: `python dynamic-bq-writes.py`. BigQuery dataset and tables were created with:

```bash
bq mk dynamic
bq mk dynamic.book name:STRING,author:STRING,type:STRING
bq mk dynamic.movie name:STRING,director:STRING,type:STRING
bq mk dynamic.album name:STRING,band:STRING,type:STRING
```

To run on Google Cloud Platform and the `DataflowRunner` set up the `PROJECT` and `BUCKET` env variables and execute. Additionally, as it is an experimental feature you'll need to add the `--experiments use_beam_bq_sink` flag (for now):

```bash
python  dynamic-bq-writes.py --runner DataflowRunner --temp_location gs://$BUCKET/temp --project $PROJECT` --experiments use_beam_bq_sink
```
Otherwise, you'll get the following error pop up before sending the job: `AttributeError: 'function' object has no attribute 'tableId'`.

This code was tested with `apache-beam[gcp]==2.12.0`. 

## Example

In our quick example we'll get some input dict data where different entries have different schema. Luckily, there is a `type` field which we can use to route the element to the appropriate destination table: 

```python
data = [{'name': 'A brave new world', 'author': 'Aldous Huxley', 'type': 'book'},
        {'name': 'Blade runner', 'director': 'Ridley Scott', 'type': 'movie'},
        {'name': 'The wall', 'band': 'Pink Floyd', 'type': 'album'},
        {'name': '1984', 'author': 'George Orwell', 'type': 'book'}]
```

This is convenient enough that in our `get_table_name` function we will simply return the content of the `type` field:

```python
def get_table_name(element):
  return 'PROJECT_ID:DATASET.' + element['type']
```

This function can be easily extended to more complex use cases. Another one would be to conduct schema validation and divert failed records to a deadletter table, etc.

Then, in our pipeline logic we just simply pass the function name to the write step:

```python
'Dynamic Writes' >> beam.io.gcp.bigquery.WriteToBigQuery(table=get_table_name)
```

Results, as expected, are:

```bash
$ bq query 'SELECT * from dynamic.book'
+-------------------+---------------+------+
|       name        |    author     | type |
+-------------------+---------------+------+
| 1984              | George Orwell | book |
| A brave new world | Aldous Huxley | book |
+-------------------+---------------+------+

$ bq query 'SELECT * from dynamic.movie'
+--------------+--------------+-------+
|     name     |   director   | type  |
+--------------+--------------+-------+
| Blade runner | Ridley Scott | movie |
+--------------+--------------+-------+

$ bq query 'SELECT * from dynamic.album'
+----------+------------+-------+
|   name   |    band    | type  |
+----------+------------+-------+
| The wall | Pink Floyd | album |
+----------+------------+-------+
```

## License

This example is provided under the Apache License 2.0.

## Issues

Report any issue to the GitHub issue tracker.
