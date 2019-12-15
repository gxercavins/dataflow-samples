package com.dataflow.samples;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.joda.time.Duration;
import org.joda.time.Instant;
import java.util.concurrent.TimeUnit;
import java.util.Date;
import java.sql.Timestamp;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;


public class PubSubGetAttribute {

	private static final Logger LOG = LoggerFactory.getLogger(PubSubGetAttribute.class);

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

		p
			.apply("Read Messages", PubsubIO.readMessagesWithAttributes().fromTopic(topic))
			.apply("Log Event ID", ParDo.of(new DoFn<PubsubMessage, String>() {
				@ProcessElement
				public void processElement(ProcessContext c) throws Exception {
					try {
						String event = c.element().getAttribute("evId");
						LOG.info("Event ID: " + event);
						c.output(event);
					}
					catch(Exception e) { }
				}
			}));

		p.run();
	}
}
