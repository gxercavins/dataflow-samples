package org.apache.beam.examples;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class BigQueryUpsert {
  
  private static final Logger LOG = LoggerFactory.getLogger(BigQueryUpsert.class);

  public static interface MyOptions extends PipelineOptions {
        @Description("Output BigQuery table <PROJECT_ID>:<DATASET>.<TABLE>")
        String getOutput();

        void setOutput(String s);
  }

  private static void MergeResults() throws Exception {
    BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
    QueryJobConfiguration queryConfig =
        QueryJobConfiguration.newBuilder(
              "MERGE upsert.full F "
            + "USING upsert.temp T "
            + "ON T.name = F.name "
            + "WHEN MATCHED THEN "
            + "UPDATE SET total = T.total "
            + "WHEN NOT MATCHED THEN "
            + "INSERT(name, total) "
            + "VALUES(name, total)")
            .setUseLegacySql(false)
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
      throw new RuntimeException(queryJob.getStatus().getError().toString());
    }
  }

  @SuppressWarnings("serial")
  public static void main(String[] args) throws Exception {
    
    MyOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(MyOptions.class);
     
    Pipeline p = Pipeline.create(options);

    String output = options.getOutput();

    // Build the table schema for the output table.
    List<TableFieldSchema> fields = new ArrayList<>();
    fields.add(new TableFieldSchema().setName("name").setType("STRING"));
    fields.add(new TableFieldSchema().setName("total").setType("INTEGER"));
    TableSchema schema = new TableSchema().setFields(fields);

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

    PipelineResult res = p.run();
    res.waitUntilFinish();
    
    if (res.getState() == PipelineResult.State.DONE) {
        LOG.info("Dataflow job is finished. Merging results...");
        MergeResults();
        LOG.info("All done :)");
    }
  }
}