package com.dataflow.samples;

import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.values.KV;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.QueryResponse;
import com.google.cloud.bigquery.TableResult;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DynamicQueries {

	private static final Logger LOG = LoggerFactory.getLogger(DynamicQueries.class);

	@SuppressWarnings("serial")
	public static void main(String[] args) {
		PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
		Pipeline p = Pipeline.create(options);

		PCollection<String> students = p.apply("Read students data", Create.of("Luke", "Leia", "Yoda", "Chewbacca").withCoder(StringUtf8Coder.of()));

		PCollection<KV<String, String>> marks = students.apply("Read marks from BigQuery", ParDo.of(new DoFn<String, KV<String, String>>() {
			@ProcessElement
			public void processElement(ProcessContext c) throws Exception {
				BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();

				QueryJobConfiguration queryConfig =
				    QueryJobConfiguration.newBuilder(
				      "SELECT name, grade "
				          + "FROM test.students "
				          + "WHERE name = "
				          + "\"" + c.element() + "\" "
				          + "LIMIT 1")
				        .setUseLegacySql(false) // Use standard SQL syntax for queries.
				        .build();

				// Create a job ID so that we can safely retry.
				JobId jobId = JobId.of(UUID.randomUUID().toString());
				Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());

				// Wait for the query to complete.
				queryJob = queryJob.waitFor();
				
				// Check for errors
				if (queryJob == null) {
				  throw new RuntimeException("Job no longer exists");
				} else if (queryJob.getStatus().getError() != null) {
				  // You can also look at queryJob.getStatus().getExecutionErrors() for all
				  // errors, not just the latest one.
				  throw new RuntimeException(queryJob.getStatus().getError().toString());
				}

				// Get the results.
				QueryResponse response = bigquery.getQueryResults(jobId);

				TableResult result = queryJob.getQueryResults();

				String mark = new String();

				for (FieldValueList row : result.iterateAll()) {
				    mark = row.get("grade").getStringValue();
				}

				c.output(KV.of(c.element(), mark));
			}
		}));

		marks.apply("Log results", ParDo.of(new DoFn<KV<String, String>, KV<String, String>>() {
			@ProcessElement
			public void processElement(ProcessContext c) throws Exception {
				LOG.info("Element: " + c.element().getKey() + " " + c.element().getValue());
				c.output(c.element());
			}
		}));

		p.run();
	}
}
