package com.dataflow.samples;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileIO.ReadMatches;
import org.apache.beam.sdk.io.FileIO.Match;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.TextIO.Write;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Keys;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.Watch;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;

public class ChronologicalOrder {

	private static final Logger Log = LoggerFactory.getLogger(ChronologicalOrder.class);

	private static final DateTimeFormatter dateTimeFormat = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");

	public static interface MyOptions extends PipelineOptions {
		@Description("Input GCS path bucket (i.e. gs://BUCKET_NAME/data/**)")
		String getPath();
		void setPath(String s);
	}

	@SuppressWarnings("serial")
	public static void main(String[] args) {
		MyOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(MyOptions.class);
		Pipeline p = Pipeline.create(options);

		String inputPath = options.getPath();

		p
			.apply(FileIO.match()
			.filepattern(inputPath)
			.continuously(
				// Check for new files every minute
				Duration.standardMinutes(1),
				// Never stop checking for new files
				Watch.Growth.<String>never()))
			.apply(FileIO.readMatches())
			.apply("Add Timestamps", ParDo.of(new DoFn<ReadableFile, KV<String, String>>() {

				@Override
				public Duration getAllowedTimestampSkew() {
					return new Duration(Long.MAX_VALUE);
				}

				@ProcessElement
				public void processElement(ProcessContext c) {
					ReadableFile file = c.element();
					String fileName = file.getMetadata().resourceId().toString();
					String lines[];

					String[] dateFields = fileName.split("/");
					Integer numElements = dateFields.length;

					String hour = dateFields[numElements - 2];
					String day = dateFields[numElements - 3];
					String month = dateFields[numElements - 4];
					String year = dateFields[numElements - 5];

					String ts = String.format("%s-%s-%s %s:00:00", year, month, day, hour);
					
					try{
						lines = file.readFullyAsUTF8String().split("\n");
					
						for (String line : lines) {
							c.outputWithTimestamp(KV.of(fileName, line), new Instant(dateTimeFormat.parseMillis(ts)));
						}
					}

					catch(IOException e){
						Log.info("failed");
					}
				}}))
			.apply(Window
				.<KV<String,String>>into(FixedWindows.of(Duration.standardHours(1)))
				.triggering(AfterWatermark.pastEndOfWindow())
				.discardingFiredPanes()
				.withAllowedLateness(Duration.ZERO))
			.apply("Log results", ParDo.of(new DoFn<KV<String, String>, Void>() {
				@ProcessElement
				public void processElement(ProcessContext c, BoundedWindow window) {
					String file = c.element().getKey();
					String value = c.element().getValue();
					String eventTime = c.timestamp().toString();

					String logString = String.format("File=%s, Line=%s, Event Time=%s, Window=%s", file, value, eventTime, window.toString());
					Log.info(logString);
				}
			}));

		p.run();
	}
}