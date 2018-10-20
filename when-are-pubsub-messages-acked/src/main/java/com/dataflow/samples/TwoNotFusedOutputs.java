package com.dataflow.samples;

import java.util.ArrayList;
import java.util.List;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.Method;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.joda.time.Duration;
import org.joda.time.Instant;
import java.util.concurrent.TimeUnit;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;


public class TwoNotFusedOutputs {
  
  public static interface MyOptions extends PipelineOptions {
    @Description("First Output BigQuery table <project_id>:<dataset_id>.<table_id>")
    String getOutput1();
    void setOutput1(String s);

    @Description("Second Output BigQuery table <project_id>:<dataset_id>.<table_id>")
    String getOutput2();
    void setOutput2(String s);

    @Description("Input subscription")
    String getInput();    
    void setInput(String s);
  }

  @SuppressWarnings("serial")
  public static void main(String[] args) {
    
    MyOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(MyOptions.class);
     
    Pipeline p = Pipeline.create(options);

    String subscription = options.getInput();
    String output1 = options.getOutput1();
    String output2 = options.getOutput2();


    // create tags for the two BigQuery output tables
    TupleTag<TableRow> Output1Tag = new TupleTag<TableRow>(){};
    TupleTag<TableRow> Output2Tag = new TupleTag<TableRow>(){};

    // Build the table schema for the output tables (same for both)
    List<TableFieldSchema> fields = new ArrayList<>();
    fields.add(new TableFieldSchema().setName("EventTime").setType("STRING"));
    fields.add(new TableFieldSchema().setName("Message").setType("STRING"));
    TableSchema schema = new TableSchema().setFields(fields);

    Duration triggeringFrequency = Duration.standardSeconds(10);

    PCollectionTuple results = p
        // read messages from Pub/Sub input subscription
        .apply("Get Messages", PubsubIO.readStrings().fromSubscription(subscription))

        // do some processing. Here we just filter out messages with less than 5 characters
         // convert message to TableRow and output it for both tables
        .apply("Process Messages", ParDo.of(new DoFn<String, String>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
              if (c.element().length() >= 5) {
                c.output(c.element());
              }
          }
        }))       

        // convert message to TableRow and output it for both tables
        .apply("Convert to TableRow", ParDo.of(new DoFn<String, TableRow>() {
            @ProcessElement
            public void processElement(ProcessContext c, MultiOutputReceiver r) {
              TableRow row = new TableRow();
              row.set("EventTime", c.timestamp().toString());              
              row.set("Message", c.element());
              r.get(Output1Tag).output(row);
              r.get(Output2Tag).output(row);
          }
        }).withOutputTags(Output1Tag, TupleTagList.of(Output2Tag)));

        // write messages to both BigQuery destination tables
        results.get(Output1Tag)
            .apply("Write Messages to Table 1", BigQueryIO.writeTableRows().to(output1)
                .withSchema(schema)
                /*
                .withMethod(Method.FILE_LOADS)
                .withTriggeringFrequency(Duration.standardMinutes(5)) 
                .withNumFileShards(1)
                */
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER));

        results.get(Output2Tag)
            .apply("Write Messages to Table 2", BigQueryIO.writeTableRows().to(output2)
                .withSchema(schema)
                /*
                .withMethod(Method.FILE_LOADS)
                .withTriggeringFrequency(Duration.standardMinutes(5)) 
                .withNumFileShards(1)
                */
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER));

    p.run();
  }
}