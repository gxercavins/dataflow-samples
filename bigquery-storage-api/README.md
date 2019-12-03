# BigQuery Storage API

The [Storage API](https://cloud.google.com/bigquery/docs/reference/storage/) provides a new stream-based option to read from BigQuery tables. It can be used with `BigQueryIO` as shown [here](https://beam.apache.org/documentation/io/built-in/google-bigquery/#storage-api) but there is not a full example yet.

Snippet written as an answer to a [StackOverflow question](https://stackoverflow.com/a/58739365/6121516).

## Quickstart

Set up [authentication](https://cloud.google.com/docs/authentication/) your preferred way, set the `PROJECT` variable and run it locally with:

```bash
mvn compile -e exec:java \
 -Dexec.mainClass=com.dataflow.samples.BigQueryStorageAPI \
      -Dexec.args="--output=hacker-stories/ \
      --runner=DirectRunner"
```

This code was tested with the Java 2.16.0 SDK and the `DirectRunner`.

Don't forget to enable the [new API](https://cloud.google.com/bigquery/docs/reference/storage/#enabling_the_api).

## Example

Throughout this example we'll read the data from a public table `bigquery-public-data:london_bicycles.cycle_stations` and we'll write each bike station to a different file. Data preview:

![Screenshot from 2019-12-03 13-34-39](https://user-images.githubusercontent.com/29493411/70051749-f6221b80-15d1-11ea-8ae7-383d17f27098.png)

To specify the read method we'll use `withMethod(Method.DIRECT_READ)`. We are only interested in the `id` and `name` fields so we'll use the `.withSelectedFields()` option:

```java
PCollection<TableRow> inputRows = p
    .apply(BigQueryIO.readTableRows()
        .from("bigquery-public-data:london_bicycles.cycle_stations")
        .withMethod(Method.DIRECT_READ)
        .withSelectedFields(Lists.newArrayList("id", "name")));
```

Then we can access each field with `.get("id")` or `.get("name")`:

```java
.apply(MapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.strings()))
    .via(tableRow -> KV.of((String) tableRow.get("id"),(String) tableRow.get("name"))))
```

and write to different files using `FileIO` and `writeDynamic()` (more details [here](https://github.com/gxercavins/dataflow-samples/tree/master/UTILS/one-row-one-file)):

```java
.apply(FileIO.<String, KV<String, String>>writeDynamic()
  .by(KV::getKey)
  .withDestinationCoder(StringUtf8Coder.of())
  .via(Contextful.fn(KV::getValue), TextIO.sink())
  .to(output)
  .withNaming(key -> FileIO.Write.defaultNaming("file-" + key, ".txt")));
```

As an example, file with `746` in the name corresponds to station with `id = 746`:

```bash
$ cat output/file-746-00000-of-00004.txt 
Lots Road, West Chelsea

$ bq query --use_legacy_sql=false "SELECT name FROM \`bigquery-public-data.london_bicycles.cycle_stations\` WHERE id = 746"
Waiting on bqjob_<ID> ... (0s) Current status: DONE   
+-------------------------+
|          name           |
+-------------------------+
| Lots Road, West Chelsea |
+-------------------------+
```

## License

This example is provided under the Apache License 2.0.

## Issues

Report any issue to the GitHub issue tracker.
