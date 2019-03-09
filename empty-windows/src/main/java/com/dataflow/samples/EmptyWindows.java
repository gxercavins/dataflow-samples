package com.dataflow.samples;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.Duration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.CountingSource;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.transforms.windowing.Window;

import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;



public class EmptyWindows {

	private static final Logger LOG = LoggerFactory.getLogger(EmptyWindows.class);

	public static interface MyOptions extends PipelineOptions {
		@Description("Input topic")
		String getInput();
		
		void setInput(String s);
	}

	@SuppressWarnings("serial")
	public static void main(String[] args) {
		MyOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(MyOptions.class);
		Pipeline p = Pipeline.create(options);

		String topic = options.getInput();

        PCollection<KV<String,Integer>> mainStream = p
        	.apply("Get Messages - Data", PubsubIO.readStrings().fromTopic(topic))
			.apply("Window Messages - Data", Window.<String>into(
					SlidingWindows.of(Duration.standardMinutes(1))
							.every(Duration.standardSeconds(5)))
					.triggering(AfterWatermark.pastEndOfWindow())
					.withAllowedLateness(Duration.ZERO)
					.accumulatingFiredPanes())
			.apply("Count Messages - Data", ParDo.of(new DoFn<String, KV<String, Integer>>() {
				@ProcessElement
				public void processElement(ProcessContext c) throws Exception {
					//LOG.info("New data element in main output");
					c.output(KV.of("num_messages", 1));
				}
			}));

		PCollection<KV<String,Integer>> dummyStream = p
			.apply("Generate Sequence", GenerateSequence.from(0).withRate(1, Duration.standardSeconds(5)))
			.apply("Window Messages - Dummy", Window.<Long>into(
					SlidingWindows.of(Duration.standardMinutes(1))
							.every(Duration.standardSeconds(5)))
					.triggering(AfterWatermark.pastEndOfWindow())
					.withAllowedLateness(Duration.ZERO)
					.accumulatingFiredPanes())
			.apply("Count Messages - Dummy", ParDo.of(new DoFn<Long, KV<String, Integer>>() {
				@ProcessElement
				public void processElement(ProcessContext c) throws Exception {
					//LOG.info("New dummy element in main output");
					c.output(KV.of("num_messages", 0));
				}
			}));

		final TupleTag<Integer> dummyTag = new TupleTag<>();
		final TupleTag<Integer> dataTag = new TupleTag<>();

		PCollection<KV<String, CoGbkResult>> coGbkResultCollection = KeyedPCollectionTuple.of(dummyTag, dummyStream)
				.and(dataTag, mainStream).apply(CoGroupByKey.<String>create());

		coGbkResultCollection
				.apply("Log results", ParDo.of(new DoFn<KV<String, CoGbkResult>, Void>() {

					@ProcessElement
					public void processElement(ProcessContext c, BoundedWindow window) {
						Integer total_sum = new Integer(0);

						Iterable<Integer> tag2Val = c.element().getValue().getAll(dataTag);
						for (Integer val : tag2Val) {
							total_sum += val;
						}

						LOG.info("Window: " + window.toString() + ", Number of messages: " + total_sum.toString());
					}
				}));

		p.run();
	}
}
