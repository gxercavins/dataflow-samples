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
  }

  static class LoggingFn extends DoFn<KV<String,Iterable<String>>,TableRow> {
    @ProcessElement
    public void processElement(ProcessContext c, BoundedWindow window) {
      TableRow row = new TableRow();
      PaneInfo pane = c.pane();
      Integer key = c.element().getKey().hashCode(); //hashed for data privacy
      Iterable<String> values = c.element().getValue();
      Date date= new Date();
      Long time = date.getTime();
      String processingTime = new Instant(time).toString();
      String eventTime = c.timestamp().toString();
      String logString = String.format("key=%s, numElements=%d, window=%s, Pane: [isFirst=%s, isLast=%s, timing=%s], eventTime=%s, processingTime=%s", key, Iterators.size(values.iterator()), window.toString(), pane.isFirst(), pane.isLast(), pane.getTiming(), eventTime, processingTime);
      LOG.info(logString);

      row.set("Key", key);
      row.set("NumberOfElements", Iterators.size(values.iterator()));
      row.set("Window", window.toString());              
      row.set("PaneIsFirst", pane.isFirst());
      row.set("PaneIsLast", pane.isLast());
      row.set("PaneTiming", pane.getTiming());
      row.set("EventTime", eventTime);
      row.set("ProcessingTime", processingTime);
      c.output(row);
    }
  }

  @SuppressWarnings("serial")
  public static void main(String[] args) {
    
    MyOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(MyOptions.class);
    //options.setStreaming(true);
    
    // Overrides the default log level on the worker to emit logs at TRACE or higher.
    //DataflowWorkerLoggingOptions loggingOptions = options.as(DataflowWorkerLoggingOptions.class);
    //loggingOptions.setDefaultWorkerLogLevel(Level.TRACE);
     
    Pipeline p = Pipeline.create(options);

    String topic = options.getInput();
    String output = options.getOutput();

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
    LOG.info("Starting job");

    p //
        .apply("GetMessages", PubsubIO.readStrings().fromTopic(topic)) //
        .apply("window",
            Window.<String>into(FixedWindows //
                .of(Duration.standardMinutes(1))) //
                .triggering(
                      AfterWatermark.pastEndOfWindow()
                          .withLateFirings(AfterProcessingTime
                             .pastFirstElementInPane()))
                .withAllowedLateness(Duration.standardMinutes(1))
                .accumulatingFiredPanes()) //
        //will just use first word in sentence as key
        .apply("Create Keys", ParDo.of(new DoFn<String, KV<String,String>>() {
            @ProcessElement
            public void processElement(ProcessContext c, BoundedWindow window) {
              LOG.info("New element into pipeline");
              c.output(KV.of(c.element().split(" ")[0],c.element()));
          }
        }))
        .apply("Group By Key", GroupByKey.<String,String>create())
        .apply(ParDo.of(new LoggingFn()))           
        .apply(BigQueryIO.writeTableRows().to(output)//
            .withSchema(schema)//
            .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
            .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));

    p.run();
  }
}