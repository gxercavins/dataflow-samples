# Enrich with BigQuery

This example aims to show how to retrieve data from BigQuery to enrich an existing PCollection. It was written originally as an answer to a StackOverflow [question](https://stackoverflow.com/questions/53202450/how-to-read-bigquery-from-pcollection-in-dataflow/).

## Quickstart

Set up [authentication](https://cloud.google.com/docs/authentication/) your preferred way. You can use the provided `local.sh` script to create the BigQuery data and launch the Dataflow job locally. Run it as:

```bash
./local.sh PROJECT_ID
```

This code was tested with Java SDK 2.5.0 and the 1.22.0 version of the BigQuery Java Client Library.

## Example

`BigQueryIO` is intended to read from BigQuery as a source at the start of the pipeline. In this scenario, we can still to read the whole table and materialize it as a side input. Another possibility, the one I implemented herein, is to use the Java Client Library directly from within an intermediate step.

We can start by creating the BigQuery table with data. In our case, we'll want to retrieve the grade for each 'student' that we might find in the PCollection record that we are processing:

```bash
bq mk test.students name:STRING,grade:STRING
bq query --use_legacy_sql=false 'insert into test.students (name, grade) values ("Yoda", "A+"), ("Leia", "B+"), ("Luke", "C-"), ("Chewbacca", "F")'
```

the table looks like this:

[![enter image description here][1]][1]

Then, within the pipeline, we first generate some input dummy data. Each input element has its corresponding match in the BigQuery table:

```java
Create.of("Luke", "Leia", "Yoda", "Chewbacca")
```

For each one of them we fetch the corresponding grade in the BigQuery table following the approach in [this example][2]. The following snippet will create a query job with a different student in the filter predicate each time (notice that `c.element()` is part of the `WHERE` clause):

```java
// ParDo to map each student with the grade in BigQuery
PCollection<KV<String, String>> marks = students.apply("Read marks from BigQuery", ParDo.of(new DoFn<String, KV<String, String>>() {
	@ProcessElement
	public void processElement(ProcessContext c) throws Exception {
		BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();

		QueryJobConfiguration queryConfig =
		    QueryJobConfiguration.newBuilder(
		      "SELECT grade "
		          + "FROM `PROJECT_ID.test.students` "
		          + "WHERE name = "
		          + "\"" + c.element() + "\" "  // fetch the appropriate student
		          + "LIMIT 1")
		        .setUseLegacySql(false) // Use standard SQL syntax for queries.
		        .build();

		// Create a job ID so that we can safely retry.
		JobId jobId = JobId.of(UUID.randomUUID().toString());
		Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());
```

After that, we wait for the job to complete and get the results. The output of this stage will be a key-value pair where, for the key, we just pass forward the processed elemt (the student) and, as value, we output the grade retrieved from BigQuery:

```java
		// Wait for the query to complete.
		queryJob = queryJob.waitFor();

		// Check for errors
		if (queryJob == null) {
		  throw new RuntimeException("Job no longer exists");
		} else if (queryJob.getStatus().getError() != null) {
		  throw new RuntimeException(queryJob.getStatus().getError().toString());
		}

		// Get the results.
		QueryResponse response = bigquery.getQueryResults(jobId)
		TableResult result = queryJob.getQueryResults();

		String mark = new String();

		for (FieldValueList row : result.iterateAll()) {
		    mark = row.get("grade").getStringValue();
		}

		c.output(KV.of(c.element(), mark));
	}
}));
```

Finally, we'll just log the results to verify the expected behavior:

```java 
// log to check everything is right
marks.apply("Log results", ParDo.of(new DoFn<KV<String, String>, KV<String, String>>() {
	@ProcessElement
	public void processElement(ProcessContext c) throws Exception {
		LOG.info("Element: " + c.element().getKey() + " " + c.element().getValue());
		c.output(c.element());
	}
}));
```

If we run it locally the logs will be printed out to `stdout` and should be something like:

```bash
Nov 08, 2018 2:17:16 PM com.dataflow.samples.DynamicQueries$2 processElement
INFO: Element: Yoda A+
Nov 08, 2018 2:17:16 PM com.dataflow.samples.DynamicQueries$2 processElement
INFO: Element: Luke C-
Nov 08, 2018 2:17:16 PM com.dataflow.samples.DynamicQueries$2 processElement
INFO: Element: Chewbacca F
Nov 08, 2018 2:17:16 PM com.dataflow.samples.DynamicQueries$2 processElement
INFO: Element: Leia B+
```

For each element we'll create a BigQuery job. Take into account, depending on your data volume, quotas and cost considerations, you might need to throttle the query step to limit API calls rate.

  [1]: https://i.stack.imgur.com/5Bh9m.png
  [2]: https://github.com/GoogleCloudPlatform/java-docs-samples/blob/master/bigquery/cloud-client/src/main/java/com/example/bigquery/SimpleApp.java

## License

These examples are provided under the Apache License 2.0.

## Issues

Report any issue to the GitHub issue tracker.
