package com.dataflow.samples;

import static com.google.common.base.MoreObjects.firstNonNull;

import com.google.common.collect.Iterables;
import com.google.gson.Gson;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.*;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class SODemoQuestion {
   private static final Gson GSON = new Gson();
   private static final Logger LOG = LoggerFactory.getLogger(SODemoQuestion.class);


   public static void main(String[] args)
   {
      Integer NUM_ELEMENTS = 10;
      String topic = "projects/PROJECT_ID/topics/TOPIC";
      PipelineOptions opts = PipelineOptionsFactory.fromArgs().create();
      Pipeline pipeline = Pipeline.create(opts);
      PCollection<String> queue = pipeline
         .apply("ReadQueue", PubsubIO.readStrings().fromTopic(topic))
         .apply(Window
            .<String>into(FixedWindows.of(Duration.standardSeconds(1)))
            .withAllowedLateness(Duration.standardSeconds(3))
            .triggering(AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.standardSeconds(2)))
            .discardingFiredPanes());

      TupleTag<List<KV<Integer, Integer>>> tagDeserialized = new TupleTag<List<KV<Integer, Integer>>>() {};
      TupleTag<Integer> tagDeserializeError = new TupleTag<Integer>() {};
      PCollectionTuple imagesInputTuple = queue
         .apply("DeserializeJSON", ParDo.of(new DoFn<String, List<KV<Integer, Integer>>>() {
            @ProcessElement
            public void processElement(@Element String input, OutputReceiver<List<KV<Integer, Integer>>> output)
            {
               List<Double> list = GSON.fromJson(input, List.class);
               // hack some 'unique' id (should really be part of each T in the input array
               Integer key = Math.toIntExact(System.currentTimeMillis() - 1575300000000L);

               List<KV<Integer, Integer>> out = new ArrayList<>();
               for (Double el : list) {
                  out.add(KV.of(key, el.intValue()));
               }
               output.output(out);
            }

         }).withOutputTags(tagDeserialized, TupleTagList.of(tagDeserializeError)));

      PCollection<KV<Integer, Integer>> images = imagesInputTuple.get(tagDeserialized)
//       .apply(Window.<List<KV<Integer, Integer>>>into(new GlobalWindows()))
         .apply("Flatten into timestamp", ParDo.of(new DoFn<List<KV<Integer, Integer>>, KV<Integer, Integer>>() {
            // Flatten and output into same ts
            @ProcessElement
            public void processElement(@Element List<KV<Integer, Integer>> input, OutputReceiver<KV<Integer, Integer>> out, @Timestamp Instant ts, BoundedWindow w, PaneInfo p) {
               Instant timestamp = w.maxTimestamp();
               for (KV<Integer, Integer> el : input) {
                  out.outputWithTimestamp(el, timestamp);
               }
            }
         })).apply(Window.<KV<Integer, Integer>>into(new GlobalWindows()));

      TupleTag<KV<Integer, Integer>> tagProcessed = new TupleTag<KV<Integer, Integer>>() {};
      TupleTag<KV<Integer, Integer>> tagError = new TupleTag<KV<Integer, Integer>>() {};
      PCollectionTuple processed = images.apply("ProcessingStep", ParDo.of(new DoFn<KV<Integer, Integer>, KV<Integer, Integer>>() {
         // A state bag holding all elements seen for that key
         @StateId("elements_seen")
         private final StateSpec<BagState<Integer>> elementSpec =
               StateSpecs.bag();

         // A state cell holding error count
         @StateId("errors")
         private final StateSpec<ValueState<Integer>> errorSpec =
               StateSpecs.value(VarIntCoder.of());

         @ProcessElement
         public void processElement(@Element KV<Integer, Integer> input, MultiOutputReceiver output, @StateId("elements_seen") BagState<Integer> state,
            @StateId("errors") ValueState<Integer> errors) {
            Boolean is_error = false;

            if (input.getValue() % 4 == 0) {
               // skip processing
               LOG.info("skip multiples of four: {}", input);
            } else if (input.getValue() % 6 == 0) {
               try {
                  // simulation of "processing time" without actual network use
                  Thread.sleep(5000);
               } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
               }
               LOG.info("multiple of 6 slow: {}", input);
            } else if (input.getValue() % 7 == 0) {
               LOG.warn("multiple of 7 error: {}", input);
               errors.write(firstNonNull(errors.read(), 0) + 1);
               is_error = true;
               output.get(tagError).output(input);
            } else {
               // simulate processing
               LOG.info("lengthy process {}", input);
               try {
                  Thread.sleep(1000);
               } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
               }
            }

            int count = firstNonNull(Iterables.size(state.read()), 0) + firstNonNull(errors.read(), 0);
            
            if (!is_error) {
               state.add(input.getValue());
               count += 1;
            }

            if (count >= NUM_ELEMENTS) {
               Iterable<Integer> all_elements = state.read();
               Integer key = input.getKey();

               for (Integer value : all_elements) {
                  output.get(tagProcessed).output(KV.of(key, value));
               }
            }
         }
      }).withOutputTags(tagProcessed, TupleTagList.of(tagError)));

      PCollection end = processed.get(tagProcessed)
         .apply("GroupByParentId", GroupByKey.create())
         .apply("GroupedValues", Values.create())
         .apply("PublishSerialize", ParDo.of(
            new DoFn<Object, String>() {
               @ProcessElement
               public void processElement(ProcessContext pc) {
                  String output = GSON.toJson(pc.element());
                  LOG.info("DONE: {}", output);
                  pc.output(output);
               }
            }));
      // "send the string to pubsub" goes here
      PipelineResult result = pipeline.run();
      PipelineResult.State finalStatus = result.waitUntilFinish();
      System.out.println(finalStatus);
   }
}
