# Load files with non-standard separators into BigQuery

When loading a file into BigQuery we can specify a custom delimiter but it only accepts single chars ([docs][1]):

> "The separator can be any ISO-8859-1 single-byte character."

We can use Dataflow as the middle man to specify separators such as `\x01\n`. This solution is extended from this [StackOverflow answer](https://stackoverflow.com/a/54751032/6121516) and uses a template that conveniently accepts a `delimiter` parameter. We can even set a NoOps workflow with a Cloud Function, triggered by GCS uploads, so that we automatically invoke the template for each new file.

Note that alternatives are also discussed there but herein we'll focus in the Dataflow one only.

## Quickstart

You can use the provided `stage.sh` script to create the template:

``` bash
./stage.sh <PROJECT_ID> <BUCKET_NAME>
```

and then execute it with the `run.sh` script:

```bash
./run.sh <BUCKET> gs://<BUCKET/PATH/TO/FILE> <BQ_PROJECT:DATASET.TABLE> <DELIMITER>
```

For example,

```bash
./run.sh my-bucket gs://my-bucket/separator.csv my-project:dataflow_test.separator_dataflow "\x01\n"
```

This code was tested with Java SDK 2.16.0 and the `DataflowRunner`.

## Example

Briefly, we read the records from the file using `TextIO.read().from(file)` where `file` is the GCS path (provide `input` and `output` parameters when launching the job). We can use additionally a dummy delimiter using `withDelimiter()` to avoid conflicts (here we are limited again to single bytes so we can't directly pass the real one). 

Then for each line we split by the real delimiter with `c.element().split(delimiter.get().replace("\\", "\\\\"))`. Note that we need to escape already-escaped characters (you can verify that in the JSON query results with a normal load), hence the quadruple backslashes:

```java
p
	.apply("GetMessages", TextIO.read().from(file))
        .apply("ExtractRows", ParDo.of(new DoFn<String, String>() {
        @ProcessElement
        public void processElement(ProcessContext c) {
          for (String line : c.element().split(delimiter.get().replace("\\", "\\\\"))) {
            if (!line.isEmpty()) {
              c.output(line);
            }
          }
        }
    }))
```

Results:

[![enter image description here][2]][2]

Keep in mind that you can run into problems for very large single-row CSVs either due to row limit in BigQuery or unsplittable steps assigned to a single worker in Dataflow.

  [1]: https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-csv#csv-options
  [2]: https://i.stack.imgur.com/kbFjq.png

## License

These examples are provided under the Apache License 2.0.

## Issues

Report any issue to the GitHub issue tracker.
