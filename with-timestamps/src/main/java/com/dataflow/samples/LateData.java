package com.dataflow.samples;

import java.util.ArrayList;
import java.util.List;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.WithTimestamps;
import org.joda.time.Duration;
import org.joda.time.DateTime;
import org.joda.time.Instant;
import java.util.concurrent.TimeUnit;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;


public class LateData {

    public static interface MyOptions extends DataflowPipelineOptions {
        @Description("Output BigQuery table <project_id>:<dataset_id>.<table_id>")
        String getOutput();

        void setOutput(String s);

        @Description("Input topic")
        String getInput();

        void setInput(String s);
    }

    @SuppressWarnings("serial")
    public static void main(String[] args) {
        MyOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(MyOptions.class);
        options.setStreaming(true);

        Pipeline p = Pipeline.create(options);

        String topic = options.getInput();
        String output = options.getOutput();

        // Build the table schema for the output table.
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("words").setType("STRING"));
        fields.add(new TableFieldSchema().setName("timestamp").setType("STRING"));
        TableSchema schema = new TableSchema().setFields(fields);
        
        // Assign current time as timestamp unless message starts with 'late', in which case assign a two-minute earlier timestamp
        SerializableFunction<String, Instant> timestampFn = input -> input.split(" ")[0].toString().equals("late") ? new Instant(new DateTime().minus(Duration.standardMinutes(2))) : new Instant(new DateTime());

        p
                .apply("GetMessages", PubsubIO.readStrings().fromTopic(topic))
                // assign timestamps and allow skew for messages that arrive already behind the watermark
                .apply("Timestamps", WithTimestamps.of(timestampFn).withAllowedTimestampSkew(new Duration(Long.MAX_VALUE)))
                .apply("Window",
                        Window.<String>into(FixedWindows
                                .of(Duration.standardMinutes(1)))
                                .withAllowedLateness(Duration.standardMinutes(10))
                                .triggering(
                                  AfterWatermark.pastEndOfWindow()
                                      .withLateFirings(AfterProcessingTime
                                         .pastFirstElementInPane()))
                                .accumulatingFiredPanes())
                .apply("ToBQRow", ParDo.of(new DoFn<String, TableRow>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) throws Exception {
                        TableRow row = new TableRow();
                        row.set("words", c.element());
                        row.set("timestamp", c.timestamp().toString());
                        c.output(row);
                    }
                }))
                .apply(BigQueryIO.writeTableRows().to(output)
                        .withSchema(schema)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));

        p.run();
    }
}
