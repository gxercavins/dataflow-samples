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
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;


public class LogPublishTime {

  private static final Logger LOG = LoggerFactory.getLogger(LogPublishTime.class);

	public static interface MyOptions extends PipelineOptions {
		@Description("Input subscription")
		String getInput();
		
		void setInput(String s);
	}

	@SuppressWarnings("serial")
	public static void main(String[] args) {
		MyOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(MyOptions.class);
		Pipeline p = Pipeline.create(options);

		String subscription = options.getInput();

		p
			.apply("Read Messages", PubsubIO.readStrings().fromSubscription(subscription))
			.apply("Log Publish Time", ParDo.of(new DoFn<String, Void>() {
				@ProcessElement
				public void processElement(ProcessContext c) throws Exception {
					LOG.info("Message: " + c.element());
                                        LOG.info("Publish time: " + c.timestamp().toString());
                                        Date date= new Date();
                                        Long time = date.getTime();
                                        LOG.info("Processing time: " + new Instant(time).toString());
				}
			}));

		p.run();
	}
}
