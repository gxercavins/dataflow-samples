# Filename Match

Example on how to read from multiple GCS files and enrich each record according to the filename it cames from.

This was originally posted as an answer to a couple StackOverflow questions: [one](https://stackoverflow.com/questions/53404579/dataflow-apache-beam-how-to-access-current-filename-when-passing-in-pattern/) and [two](https://stackoverflow.com/questions/53485744/reading-a-few-rows-from-bigquery-as-a-side-input-getting-none/).

## Quickstart

Set up [authentication](https://cloud.google.com/docs/authentication/) your preferred way. 

To test the script locally, execute: `./run_locally BUCKET_NAME`.

This code was tested with `apache-beam==2.8.0`.

## Example

Regarding the first question about getting filenames I found a couple [existing](https://stackoverflow.com/questions/51962956/read-a-set-of-xml-files-using-google-cloud-dataflow-python-sdk) [examples](https://stackoverflow.com/questions/51433664/read-files-from-multiple-folders-in-apache-beam-and-map-outputs-to-filenames) where they also get a list of file names that match the reading glob but the end approach is different. In there they load each entire file into a single element (having filename-content pairs) which might not scale well with large files. Instead, we want to add to each line or record that we read the file that it comes from (filename-line pairs).

The following two csv files were used as input:

```bash
$ gsutil cat gs://$BUCKET/countries1.csv
id,country
1,sweden
2,spain

gsutil cat gs://$BUCKET/countries2.csv
id,country
3,italy
4,france
```

Using `GCSFileSystem.match` we can access `metadata_list` to retrieve a FileMetadata object that will contain the file path and size in bytes. In my example:

```bash
[FileMetadata(gs://BUCKET_NAME/countries1.csv, 29),
 FileMetadata(gs://BUCKET_NAME/countries2.csv, 29)]
```

For that part the code used was:

```python
result = [m.metadata_list for m in gcs.match(['gs://{}/countries*'.format(BUCKET)])]
```

We will read each of the matching files into a different PCollection. As we don't know the number of files a priori we need to create programmatically a list of names for each PCollection `(p0, p1, ..., pN-1)` and ensure that we have unique labels for each step `('Read file 0', 'Read file 1', etc.)`:

```python
variables = ['p{}'.format(i) for i in range(len(result))]
read_labels = ['Read file {}'.format(i) for i in range(len(result))]
add_filename_labels = ['Add filename {}'.format(i) for i in range(len(result))]
```

Then we proceed to read each different file into its corresponding PCollection with `ReadFromText` and call the `AddFilenamesFn` ParDo to associate each record with the filename.

```python
for i in range(len(result)):   
  globals()[variables[i]] = p | read_labels[i] >> ReadFromText(result[i].path) | add_filename_labels[i] >> beam.ParDo(AddFilenamesFn(), result[i].path)
```

where `AddFilenamesFn` is:

```python
class AddFilenamesFn(beam.DoFn):
    """ParDo to output a dict with filename and row"""
    def process(self, element, file_path):
        file_name = file_path.split("/")[-1]
        yield {'filename':file_name, 'row':element}
```

My first approach was using a Map function directly which results in simpler code. However, `result[i].path` was resolved at the end of the loop and each record was incorrectly mapped to the last file of the list. Don't use this:

```python
globals()[variables[i]] = p | read_labels[i] >> ReadFromText(result[i].path) | add_filename_labels[i] >> beam.Map(lambda elem: (result[i].path, elem))
```

Finally, we flatten all the PCollections into a single one:

```python
merged = [globals()[variables[i]] for i in range(len(result))] | 'Flatten PCollections' >> beam.Flatten()
```

and we check the results by logging the elements:

```python
INFO:root:{'filename': u'countries2.csv', 'row': u'id,country'}
INFO:root:{'filename': u'countries2.csv', 'row': u'3,italy'}
INFO:root:{'filename': u'countries2.csv', 'row': u'4,france'}
INFO:root:{'filename': u'countries1.csv', 'row': u'id,country'}
INFO:root:{'filename': u'countries1.csv', 'row': u'1,sweden'}
INFO:root:{'filename': u'countries1.csv', 'row': u'2,spain'}
```

Now, we factor in the second part. Adding `BigQuery` data to enrich the PCollection. We want a different query for each filename.

I think that the easiest solution, taking into account the previous part, would be to run the queries inside the `AddFilenamesFn` ParDo within the for loop. Keep in mind that `beam.io.Read(beam.io.BigQuerySource(query=bqquery))` is used to read rows as source and not in an intermediate step. So, in the case we can use the Python Client Library directly (`google-cloud-bigquery>0.27.0`).

We will use a new BigQuery table:

```bash
bq mk test.file_mapping FILENAME:STRING,FILE_ID:STRING
bq query --use_legacy_sql=false 'INSERT INTO test.file_mapping (FILENAME, FILE_ID) values ("countries1.csv", "COUNTRIES ONE"), ("countries2.csv", "COUNTRIES TWO")'
```

[![enter image description here][1]][1]

and the following revised code:

```python
class AddFilenamesFn(beam.DoFn):
    """ParDo to output a dict with file id (retrieved from BigQuery) and row"""
    def process(self, element, file_path):
        from google.cloud import bigquery

        client = bigquery.Client()
        file_name = file_path.split("/")[-1]

        query_job = client.query("""
            SELECT FILE_ID
            FROM test.file_mapping
            WHERE FILENAME = '{0}'
            LIMIT 1""".format(file_name))

        results = query_job.result()

        for row in results:
          file_id = row.FILE_ID

        yield {'filename':file_id, 'row':element}
```

This would be the most straight-forward solution to implement but it might arise an issue. Instead of running all 20 or so possible queries at the start of the pipeline we are running a query for each line/record. For example, if we have 3,000 elements in a single file the same query will be launched 3,000 times. However, each different query should be actually run only once and subsequent query "repeats" will hit the [cache](https://cloud.google.com/bigquery/docs/cached-results#disabling_retrieval_of_cached_results). 

Also note that cached queries do not contribute towards the interactive query [limit](https://cloud.google.com/bigquery/quotas#query_jobs).

and the output is now:

```bash
INFO:root:{'filename': u'COUNTRIES ONE', 'row': u'id,country'}
INFO:root:{'filename': u'COUNTRIES ONE', 'row': u'1,sweden'}
INFO:root:{'filename': u'COUNTRIES ONE', 'row': u'2,spain'}
INFO:root:{'filename': u'COUNTRIES TWO', 'row': u'id,country'}
INFO:root:{'filename': u'COUNTRIES TWO', 'row': u'3,italy'}
INFO:root:{'filename': u'COUNTRIES TWO', 'row': u'4,france'}
```

Another solution would be to load all the table and materialize it as a side input (depending on size this can be problematic of course) with `beam.io.BigQuerySource()` or break it down into N queries and save each one into a different side input. Then you could select the appropriate one for each record and pass it as an additional input to `AddFilenamesFn`.

  [1]: https://i.stack.imgur.com/m8qrR.png


## License

This example is provided under the Apache License 2.0.

## Issues

Report any issue to the GitHub issue tracker.
