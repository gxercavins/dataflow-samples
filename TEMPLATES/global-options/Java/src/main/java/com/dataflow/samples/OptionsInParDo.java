package com.dataflow.samples;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@SuppressWarnings("serial")
public class OptionsInParDo {
  private static final Logger LOG = LoggerFactory.getLogger(OptionsInParDo.class);

  static String orgId;

  public interface MyOptions extends PipelineOptions {
     @Description("Org Id")
     @Default.String("123-984-a")
     ValueProvider<String> getOrgId();
     void setOrgId(ValueProvider<String> orgID);
  }

  static class CustomFn extends DoFn<String, String> {
      // access options from wihtin the ParDo
      ValueProvider<String> orgId;
      public CustomFn(ValueProvider<String> orgId) {
        this.orgId = orgId;
      }

      @ProcessElement
      public void processElement(ProcessContext c) {
          LOG.info("Hello? ");
          LOG.info("ORG ID: " + orgId.get());
      }
  }

  public static void main(String[] args) {

    PipelineOptionsFactory.register(MyOptions.class);

    final MyOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().create()
        .as(MyOptions.class);

    ValueProvider<String> orgId = options.getOrgId();
    LOG.info("orgId: " + orgId);

    Pipeline p = Pipeline.create(options);

    PCollection<String> someDataRows = p.apply("Test data", Create.of(
      "string 1", "string2", "string 3"
    ));

    someDataRows.apply("Package into a list", ParDo.of(new CustomFn(orgId)));

    p.run();
  }
}