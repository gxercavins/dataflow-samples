package com.dataflow.samples;

import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public abstract class MultipleTopics {

   private static final Logger Log = LoggerFactory.getLogger(MultipleTopics.class);

   public interface Options extends PipelineOptions {
      @Description("List of comma-separated Pub/Sub topics")
      String getTopicList();

      void setTopicList(String s);
   }

   public static void main(String[] args) { 
      Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

      String[] listOfTopicStr = options.getTopicList().split(",");

      Pipeline pipeline = Pipeline.create(options);

      PCollection[] p = new PCollection[listOfTopicStr.length];

      for (int i = 0; i < listOfTopicStr.length; i++) {
         p[i] = pipeline
            .apply(PubsubIO.readStrings().fromTopic(listOfTopicStr[i]))
            .apply(ParDo.of(new DoFn<String, Void>() {
               @ProcessElement
               public void processElement(ProcessContext c) throws Exception {
                  Log.info(String.format("Message=%s", c.element()));
               }
            }));
      }

      pipeline.run().waitUntilFinish();
   }
}