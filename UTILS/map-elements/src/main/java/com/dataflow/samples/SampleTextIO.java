package com.dataflow.samples;

import java.util.Arrays;
import java.util.List;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SampleTextIO {

	private static final Logger Log = LoggerFactory.getLogger(SampleTextIO.class);

    // define AddFieldFn extending from SimpleFunction and overriding apply method
    static class AddFieldFn extends SimpleFunction<String, List<String>> {
	    @Override
	    public List<String> apply(String word) {
	        return Arrays.asList(word, "Its weekend!");
	    }
	}

    // just print the results
	static class PrintResultsFn extends DoFn<List<String>, Void> {
        @ProcessElement
        public void processElement(@Element List<String> words) {
            Log.info(Arrays.toString(words.toArray()));
        }
    }

    public static void main ( String[] args ) {
        Log.info( "Main class for DirectRunner" );

        // Pipeline create using default runner (DirectRunnter)
        // Interface: PipelineOptions
        PipelineOptions options = PipelineOptionsFactory.create();

        Pipeline p = Pipeline.create(options);

        // Example pcollection
        final List<String> LINES = Arrays.asList(
            "blah"
        );

        // via ProcessFunction
        PCollection p1 = p.apply(Create.of(LINES))
          .apply(MapElements.into(TypeDescriptors.lists(TypeDescriptors.strings()))
            			    .via((String word) -> (Arrays.asList(word, "Its weekend!"))))
          .apply(ParDo.of(new PrintResultsFn()));

        // via in-line SimpleFunction
        PCollection p2 = p.apply(Create.of(LINES))
          .apply(MapElements.via(new SimpleFunction<String, List<String>>() {
	        public List<String> apply(String word) {
	          return Arrays.asList(word, "Its weekend!");
	        }}))
          .apply(ParDo.of(new PrintResultsFn()));

        // via AddFieldFn class 
        PCollection p3 = p.apply(Create.of(LINES))
          .apply(MapElements.via(new AddFieldFn()))
          .apply(ParDo.of(new PrintResultsFn()));

        p.run().waitUntilFinish();
    }
}