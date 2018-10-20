package com.dataflow.samples;

import java.util.ArrayList;
import java.util.List;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;


public class FusedPipeline {
  
  public static interface MyOptions extends PipelineOptions {
    @Description("Output topic")
    String getOutput();
    void setOutput(String s);

    @Description("Input subscription")
    String getInput();    
    void setInput(String s);
  }

  @SuppressWarnings("serial")
  public static void main(String[] args) {
    
    MyOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(MyOptions.class);
     
    Pipeline p = Pipeline.create(options);

    String subscription = options.getInput();
    String output = options.getOutput();

    p
        // read messages from Pub/Sub input subscription
        .apply("Read Messages", PubsubIO.readStrings().fromSubscription(subscription))

        // do some processing. Here we just filter out messages with less than 5 characters
        .apply("Process Messages", ParDo.of(new DoFn<String, String>() {
            @ProcessElement
            public void processElement(ProcessContext c) throws Exception {
              if (c.element().length() >= 5) {
                c.output(c.element());
              }
              else {
                throw new Exception();
              }
          }
        }))       

        // write messages to Pub/Sub output topic
        .apply("Write Messages", PubsubIO.writeStrings().to(output));

    p.run();
  }
}