package com.dataflow.samples;

import java.util.ArrayList;
import java.util.List;
import java.util.Date;
import java.sql.Timestamp;
import avro.shaded.com.google.common.collect.Iterators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.joda.time.Duration;
import org.joda.time.Instant;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
//import org.apache.beam.runners.dataflow.options.DataflowWorkerLoggingOptions;
//import org.apache.beam.runners.dataflow.options.DataflowWorkerLoggingOptions.Level;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;


public class LogGBK {
  
  private static final Logger LOG = LoggerFactory.getLogger(LogGBK.class);

  public static interface MyOptions extends PipelineOptions {
    @Description("Output BigQuery table <project_id>:<dataset_id>.<table_id>")
    String getOutput();
    void setOutput(String s);

    @Description("Input topic")
    String getInput();    
    void setInput(String s);

    @Description("Option to enable or disable logging to Stackdriver: true (default) or false")
    @Default.Boolean(true)
    Boolean getStackdriver();
    void setStackdriver(Boolean s);

    @Description("Option to enable or disable logging to BigQuery: true (default) or false")
    @Default.Boolean(true)
    Boolean getBigquery();
    void setBigquery(Boolean s);
  }

  // create tags for the main and side output to write logged elements to BigQuery
  public static final TupleTag<KV<String,Iterable<String>>> mainOutputTag = new TupleTag<KV<String,Iterable<String>>>(){};
  public static final TupleTag<TableRow> logToBigQueryTag = new TupleTag<TableRow>(){};

  // function that will log info about grouped data
  static class LoggingFn extends DoFn<KV<String,Iterable<String>>,KV<String,Iterable<String>>> {
      // access options to enable/disable Stackdriver Logging and BigQuery
      private final Boolean stackdriver;
      private final Boolean bigquery;
      public LoggingFn(Boolean stackdriver, Boolean bigquery) {
          this.stackdriver = stackdriver;
          this.bigquery = bigquery;
      }
      @ProcessElement
      public void processElement(ProcessContext c, BoundedWindow window, MultiOutputReceiver r) {
          TableRow row = new TableRow();
          PaneInfo pane = c.pane();
          Integer key = c.element().getKey().hashCode();    // hashed for data privacy
          Iterable<String> values = c.element().getValue();
          Date date= new Date();
          Long time = date.getTime();
          String processingTime = new Instant(time).toString();
          String eventTime = c.timestamp().toString();
          
          // if enabled, log entries to Stackdriver Logging
          if (stackdriver) {
              String logString = String.format("key=%s, numElements=%d, window=%s, Pane: [isFirst=%s, isLast=%s, timing=%s], eventTime=%s, processingTime=%s", key, Iterators.size(values.iterator()), window.toString(), pane.isFirst(), pane.isLast(), pane.getTiming(), eventTime, processingTime);
              LOG.info(logString);
          }

          // if enabled, divert log entries to BigQuery using a side output
          if (bigquery) {
              row.set("Key", key);
              row.set("NumberOfElements", Iterators.size(values.iterator()));
              row.set("Window", window.toString());              
              row.set("PaneIsFirst", pane.isFirst());
              row.set("PaneIsLast", pane.isLast());
              row.set("PaneTiming", pane.getTiming());
              row.set("EventTime", eventTime);
              row.set("ProcessingTime", processingTime);

              r.get(logToBigQueryTag).output(row);
          }
          
          r.get(mainOutputTag).output(c.element());
      }
  }

  @SuppressWarnings("serial")
  public static void main(String[] args) {
    
    MyOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(MyOptions.class);
    //options.setStreaming(true);
    
    // Overrides the default log level on the worker to emit logs at TRACE or higher.
    // Uncomment the following two lines (and the two additional imports) if needed but beware of log verbosity
    // DataflowWorkerLoggingOptions loggingOptions = options.as(DataflowWorkerLoggingOptions.class);
    // loggingOptions.setDefaultWorkerLogLevel(Level.TRACE);
     
    Pipeline p = Pipeline.create(options);

    String topic = options.getInput();
    String output = options.getOutput();
    Boolean stackdriver = options.getStackdriver();
    Boolean bigquery = options.getBigquery();

    // Build the table schema for the output table.
    List<TableFieldSchema> fields = new ArrayList<>();
    fields.add(new TableFieldSchema().setName("Key").setType("INTEGER"));
    fields.add(new TableFieldSchema().setName("NumberOfElements").setType("INTEGER"));
    fields.add(new TableFieldSchema().setName("Window").setType("STRING"));
    fields.add(new TableFieldSchema().setName("PaneIsFirst").setType("BOOLEAN"));
    fields.add(new TableFieldSchema().setName("PaneIsLast").setType("BOOLEAN"));
    fields.add(new TableFieldSchema().setName("PaneTiming").setType("STRING"));
    fields.add(new TableFieldSchema().setName("EventTime").setType("STRING"));
    fields.add(new TableFieldSchema().setName("ProcessingTime").setType("STRING"));
    TableSchema schema = new TableSchema().setFields(fields);

    PCollectionTuple results = p
        .apply("Get Messages", PubsubIO.readStrings().fromTopic(topic))
        .apply("Window",
            Window.<String>into(FixedWindows
                .of(Duration.standardMinutes(1)))
                .triggering(
                      AfterWatermark.pastEndOfWindow()
                          .withLateFirings(AfterProcessingTime
                             .pastFirstElementInPane()))
                .withAllowedLateness(Duration.standardMinutes(1))
                .accumulatingFiredPanes())
        // we'll just use the first word in the Pub/Sub message as the key
        .apply("Create Keys", ParDo.of(new DoFn<String, KV<String,String>>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
              c.output(KV.of(c.element().split(" ")[0],c.element()));
          }
        }))
        .apply("Group By Key", GroupByKey.<String,String>create())    // grouping by key (and 1-min windows)
        // calling the logging ParDo  
        .apply("Log Info", ParDo.of(new LoggingFn(stackdriver, bigquery))
                    .withOutputTags(mainOutputTag, TupleTagList.of(logToBigQueryTag)));

        // write logged elements to BigQuery destination table
        results.get(logToBigQueryTag)
            .apply("Write Logs", BigQueryIO.writeTableRows().to(output)
                .withSchema(schema)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));

        // continue processing main output...
        results.get(mainOutputTag)
            .apply("Continue Processing", ParDo.of(new DoFn<KV<String,Iterable<String>>, Void>() {
                @ProcessElement
                public void processElement(ProcessContext c) {
                    LOG.info("New element in main output");
                    // do something
                    // ...
            }})); 

    p.run();
  }
}