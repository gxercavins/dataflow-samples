package org.apache.beam.examples;

import java.util.Arrays;
import java.util.List;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class FlattenSideOutputs {
  
  private static final Logger LOG = LoggerFactory.getLogger(FlattenSideOutputs.class);

  public static final TupleTag<String> OK = new TupleTag<String>(){};
  public static final TupleTag<String> NOTOK = new TupleTag<String>(){};

  // split elements into OK/NOTOK side outputs
  static class ValidateFn extends DoFn<String, String> {
      @ProcessElement
      public void processElement(ProcessContext c, MultiOutputReceiver r) {
          String line = c.element();
          
          if (line.contains("good")) {
              r.get(OK).output(line);
          }
          
          else {
              r.get(NOTOK).output(line);
          }
      }
  }

  @SuppressWarnings("serial")
  public static void main(String[] args) {
    
    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(PipelineOptions.class);    
    Pipeline p = Pipeline.create(options);

    // Example data
    final List<String> LINES = Arrays.asList(
        "good line",
        "bad line"
    );

    PCollectionTuple myPCollection = p
        .apply("Create Data", Create.of(LINES))
        .apply("Split OK/NOTOK", ParDo.of(new ValidateFn())
                    .withOutputTags(OK, TupleTagList.of(NOTOK)));

    PCollection<String> okResults = myPCollection.get(OK);
    PCollection<String> notOkResults = myPCollection.get(NOTOK);

    // process correct elements
    okResults
        .apply("Process Ok Records", ParDo.of(new DoFn<String, Void>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                LOG.info(String.format("Ok element: %s", c.element()));
                // do something
        }})); 

    // process incorrect elements
    notOkResults
        .apply("Process Not Ok Records", ParDo.of(new DoFn<String, Void>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                LOG.info(String.format("Not Ok element: %s", c.element()));
                // do something
        }})); 

    // process all elements
    PCollectionList<String> pcl = PCollectionList.empty(p);
    pcl = pcl.and(okResults).and(notOkResults);
    PCollection<String> allResults = pcl.apply(Flatten.pCollections());

    allResults
        .apply("Process All Records", ParDo.of(new DoFn<String, Void>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                LOG.info(String.format("All elements: %s", c.element()));
                // do something
        }})); 

    p.run();
  }
}