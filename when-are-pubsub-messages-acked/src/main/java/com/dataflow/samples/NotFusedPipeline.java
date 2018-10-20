package com.dataflow.samples;

import java.util.ArrayList;
import java.util.List;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;


public class NotFusedPipeline {
  
  public static interface MyOptions extends PipelineOptions {
    @Description("BigQuery destination table <project_id>:<dataset_id>.<table_id>")
    String getOutput();
    void setOutput(String s);

    @Description("Input subscription")
    String getInput();    
    void setInput(String s);
  }

  @SuppressWarnings("serial")
  public static void main(String[] args) {
    
    MyOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(MyOptions.class);
     
    Pipeline p = Pipeline.create(options);

    String subscription = options.getInput();
    String output = options.getOutput();

    // Build the table schema for the output tables (same for both)
    List<TableFieldSchema> fields = new ArrayList<>();
    fields.add(new TableFieldSchema().setName("EventTime").setType("STRING"));
    fields.add(new TableFieldSchema().setName("Message").setType("STRING"));
    TableSchema schema = new TableSchema().setFields(fields);

    p
        // read messages from Pub/Sub input subscription
        .apply("Get Messages", PubsubIO.readStrings().fromSubscription(subscription))

        // do some processing. Here we just filter out messages with less than 5 characters
        .apply("Process Messages", ParDo.of(new DoFn<String, String>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
              if (c.element().length() >= 5) {
                c.output(c.element());
              }
          }
        }))       

        // convert message to TableRow
        .apply("Convert to TableRow", ParDo.of(new DoFn<String, TableRow>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
              TableRow row = new TableRow();
              row.set("EventTime", c.timestamp().toString());              
              row.set("Message", c.element());
              c.output(row);
          }
        }))

        // write messages to BigQuery destination table
        .apply("Write Messages", BigQueryIO.writeTableRows().to(output)
            .withSchema(schema)
            .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
            .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER));

    p.run();
  }
}