package com.dataflow.samples;

import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.DynamicSessions; // custom one
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DynamicSessionsExample {

  private static final Logger LOG = LoggerFactory.getLogger(DynamicSessionsExample.class);

  public static void main(String[] args) {
    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(PipelineOptions.class);

    Pipeline p = Pipeline.create(options);

    p
      .apply("Create data", Create.timestamped(
            TimestampedValue.of("{\"user\":\"mobile\",\"score\":\"12\",\"gap\":\"5\"}", new Instant()),
            TimestampedValue.of("{\"user\":\"desktop\",\"score\":\"4\"}", new Instant()),
            TimestampedValue.of("{\"user\":\"mobile\",\"score\":\"-3\",\"gap\":\"5\"}", new Instant().plus(2000)),
            TimestampedValue.of("{\"user\":\"mobile\",\"score\":\"2\",\"gap\":\"5\"}", new Instant().plus(9000)),
            TimestampedValue.of("{\"user\":\"mobile\",\"score\":\"7\",\"gap\":\"5\"}", new Instant().plus(12000)),
            TimestampedValue.of("{\"user\":\"desktop\",\"score\":\"10\"}", new Instant().plus(12000)))
        .withCoder(StringUtf8Coder.of()))
      .apply("Window into sessions", Window.<String>into(DynamicSessions
        .withDefaultGapDuration(Duration.standardSeconds(10))
        .withGapAttribute("gap"))
          .triggering(AfterWatermark.pastEndOfWindow())
          .withAllowedLateness(Duration.ZERO)
          .discardingFiredPanes())
      .apply("Assign keys", ParDo.of(new DoFn<String, KV<String, Integer>>() {
        @ProcessElement
        public void processElement(ProcessContext c) {
          JSONObject message = new JSONObject(c.element());
          String key = message.getString("user");
          int score = message.getInt("score");
          c.output(KV.of(key, score));
        }
      }))
      .apply("Sum score", Sum.<String>integersPerKey())
      .apply("Log results", ParDo.of(new DoFn<KV<String,Integer>, Void>() {
        @ProcessElement
        public void processElement(ProcessContext c, BoundedWindow window) {
          String user = c.element().getKey();
          Integer score = c.element().getValue();

          String logString = String.format("user=%s, score=%d, window=%s", user, score, window.toString());
          LOG.info(logString);
        }
      }));

    p.run();
  }
}
