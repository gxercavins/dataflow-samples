package com.dataflow.samples;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.AfterEach;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.Sessions; 
import org.apache.beam.sdk.transforms.windowing.AfterPane;

import org.apache.beam.sdk.values.KV;
import org.joda.time.Duration;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;

/**
 * Example pipeline that will fire triggers every 10 sec approx and, once past the first minute, it will resort to one trigger each minute
 * @author gxercavins
 * 
 * Original example job that reads from Pub/Sub and writes to BigQuery found here:
 * https://github.com/GoogleCloudPlatform/training-data-analyst/blob/master/courses/data_analysis/lab2/javahelp/src/main/java/com/google/cloud/training/dataanalyst/javahelp/StreamDemoConsumer.java
 * @author vlakshmanan
 */
public class AdaptiveTriggers {

	public static interface MyOptions extends DataflowPipelineOptions {
		@Description("Output BigQuery table <PROJECT_ID>:<DATASET>.<TABLE>")
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
		fields.add(new TableFieldSchema().setName("timestamp").setType("TIMESTAMP"));
		fields.add(new TableFieldSchema().setName("num_words").setType("INTEGER"));
        fields.add(new TableFieldSchema().setName("pane_index").setType("INTEGER"));
        fields.add(new TableFieldSchema().setName("pane_timing").setType("STRING"));
		TableSchema schema = new TableSchema().setFields(fields);

		p //
			.apply("GetMessages", PubsubIO.readStrings().fromTopic(topic)) //
          	.apply("window" , Window
              	.<String>into(FixedWindows.of(Duration.standardMinutes(10)))
              	.triggering(AfterWatermark.pastEndOfWindow()
                  	.withEarlyFirings(AfterProcessingTime.pastFirstElementInPane()
                    .plusDelayOf(Duration.standardSeconds(5))))
	            .accumulatingFiredPanes()
    	        .withAllowedLateness(Duration.ZERO))
              
			.apply("WordsPerLine", ParDo.of(new DoFn<String, Integer>() {
				@ProcessElement
				public void processElement(ProcessContext c) throws Exception {
					String line = c.element();
					c.output(line.split(" ").length);
				}
			}))//
			.apply("WordsInTimeWindow", Sum.integersGlobally().withoutDefaults()) //
			.apply("ToBQRow", ParDo.of(new DoFn<Integer, TableRow>() {
				@ProcessElement
				public void processElement(ProcessContext c) throws Exception {
					Long index = c.pane().getIndex();
					if (index < 6 || index % 6 == 0) {
						TableRow row = new TableRow();
						row.set("timestamp", Instant.now().toString());
						row.set("num_words", c.element());
						row.set("pane_index", index);
						row.set("pane_timing", c.pane().getTiming().toString());
						c.output(row);
					}
				}
			})) //
			.apply(BigQueryIO.writeTableRows().to(output)//
					.withSchema(schema)//
					.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
					.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));

		p.run();
	}
}
