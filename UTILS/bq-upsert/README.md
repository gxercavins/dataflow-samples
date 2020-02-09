# BigQuery Upsert

In our use case we write results to a temporary BigQuery table and, once we are done, we want to update the results in the full (source of truth) table. Of course, we can use multiple tools to coordinate/orchestrate the workflow such as Airflow or Cloud Functions. Here we want to demonstrate an example that solely relies on Dataflow.
This solution was originally written as a [StackOverflow answer](https://stackoverflow.com/a/60126629/6121516).

## Quickstart

You can use the provided scripts to run the code locally or on GCP:
```bash
./local.sh <PROJECT_ID> <BUCKET_NAME> <BQ_TEMP_TABLE>
./local.sh <PROJECT_ID> <BUCKET_NAME> <BQ_TEMP_TABLE>
```

Note that, if you use different BigQuery tables than `upsert.temp` and `upsert.full` you'll also need to change the hard-coded SQL query.

## Example

If this is a non-templated Batch job we can use [`PipelineResult.waitUntilFinish()`][1] which:

> Waits until the pipeline finishes and returns the final status.

Then we check if [`State`][2] is equal to `DONE` and proceed with the `MERGE` statement accordingly:

```java
PipelineResult res = p.run();
res.waitUntilFinish();

if (res.getState() == PipelineResult.State.DONE) {
    LOG.info("Dataflow job is finished. Merging results...");
    MergeResults();
    LOG.info("All done :)");
}
```

In order to test this we can create a BigQuery table (`upsert.full`) which will contain the final results and be updated each run:

```bash
bq mk upsert
bq mk -t upsert.full name:STRING,total:INT64
bq query --use_legacy_sql=false "INSERT upsert.full (name, total) VALUES('tv', 10), ('laptop', 20)"
```

at the start we'll populate it with a `total` of 10 TVs. But now let's imagine that we sell 5 extra TVs and, in our Dataflow job, we'll write a single row to a temporary table (`upsert.temp`) with the new corrected value (15):

```java
p
.apply("Create Data", Create.of("Start"))
.apply("Write", BigQueryIO
                .<String>write()
                .to(output)
                .withFormatFunction(
                    (String dummy) ->
                    new TableRow().set("name", "tv").set("total", 15))
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                .withSchema(schema));
```

So now we want to update the original table with the following query ([DML syntax][3]):

```sql
MERGE upsert.full F
USING upsert.temp T
ON T.name = F.name
WHEN MATCHED THEN
  UPDATE SET total = T.total
WHEN NOT MATCHED THEN
  INSERT(name, total)
  VALUES(name, total)
```

Therefore, we can use BigQuery's Java Client Library in `MergeResults`:

```java
BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
QueryJobConfiguration queryConfig =
    QueryJobConfiguration.newBuilder(
          "MERGE upsert.full F "
        + ...
        + "VALUES(name, total)")
        .setUseLegacySql(false)
        .build();

JobId jobId = JobId.of(UUID.randomUUID().toString());
Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());
```

This is based on this [snippet][4] which includes some basic error handling. Note that you'll need to add this to your `pom.xml` or equivalent:

```xml
<dependency>
  <groupId>com.google.cloud</groupId>
  <artifactId>google-cloud-bigquery</artifactId>
  <version>1.82.0</version>
</dependency>
```

and example output:

```java
INFO: 2020-02-08T11:38:56.292Z: Worker pool stopped.
Feb 08, 2020 12:39:04 PM org.apache.beam.runners.dataflow.DataflowPipelineJob logTerminalState
INFO: Job 2020-02-08_REDACTED finished with status DONE.
Feb 08, 2020 12:39:04 PM org.apache.beam.examples.BigQueryUpsert main
INFO: Dataflow job is finished. Merging results...
Feb 08, 2020 12:39:09 PM org.apache.beam.examples.BigQueryUpsert main
INFO: All done :)
```

```bash
$ bq query --use_legacy_sql=false "SELECT name,total FROM upsert.full LIMIT 10"
+--------+-------+
|  name  | total |
+--------+-------+
| tv     |    15 |
| laptop |    20 |
+--------+-------+
```

Tested with the 2.17.0 Java SDK and both the Direct and Dataflow runners.

Full code in `BigQueryUpsert.java`.


  [1]: https://beam.apache.org/releases/javadoc/2.17.0/org/apache/beam/sdk/PipelineResult.html#waitUntilFinish--
  [2]: https://beam.apache.org/releases/javadoc/2.17.0/org/apache/beam/sdk/PipelineResult.State.html
  [3]: https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax#merge_statement
  [4]: https://cloud.google.com/bigquery/docs/quickstarts/quickstart-client-libraries#running_the_query

## License

These examples are provided under the Apache License 2.0.

## Issues

Report any issue to the GitHub issue tracker.
