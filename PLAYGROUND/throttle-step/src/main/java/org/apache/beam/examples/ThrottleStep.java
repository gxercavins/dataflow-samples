package org.apache.beam.examples;

import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

// Imports the Google Cloud client NLP library
import com.google.cloud.language.v1.Document;
import com.google.cloud.language.v1.Document.Type;
import com.google.cloud.language.spi.v1.LanguageServiceClient;
import com.google.cloud.language.v1.Sentiment;


public class ThrottleStep {

  private static final Logger Log = LoggerFactory.getLogger(ThrottleStep.class);

  static class OmitEmptyLines extends DoFn<String, String> {

      @ProcessElement
      public void processElement(ProcessContext c) {
          if (c.element() != null && !c.element().isEmpty()) { 
            c.output(c.element());
          }
      }
  }

  public interface ThrottleStepOptions extends DataflowPipelineOptions {

    /**
     * By default, this example reads from a public dataset containing the text of
     * King Lear. Set this option to choose a different input file or glob.
     */
    @Description("Path of the file to read from")
    @Default.String("gs://apache-beam-samples/shakespeare/kinglear.txt")
    String getInputFile();
    void setInputFile(String value);

    /**
     * Set this required option to specify where to write the output.
     */
    @Description("Path of the file to write to")
    @Required
    String getOutput();
    void setOutput(String value);
  }

  static void runThrottleStep(ThrottleStepOptions options) {

    Pipeline p = Pipeline.create(options);

    Integer num_workers, num_threads;
    String machine_type;

    Integer desired_rate = 1; // in the example we do not want to exceed 1 request per second

    if (options.getNumWorkers() == 0) { num_workers = 1; }
    else { num_workers = options.getNumWorkers(); }

    // catch NPE
    if (options.getWorkerMachineType() != null) { 
        machine_type = options.getWorkerMachineType();
        num_threads = Integer.parseInt(machine_type.substring(machine_type.length() - 1));
    }
    else { num_threads = 1; }

    // we need to account for the max number of DoFn instances executing simultaneously
    Double sleep_time = (double)(num_workers * num_threads) / (double)(desired_rate);

    /* in the future we might need to account for autoscaling
    if (options.getAutoscalingAlgorithm().toString() == null) { autoscaling = "NONE"; }
    else { autoscaling = options.getAutoscalingAlgorithm().toString(); }
    */

    p
        .apply("Read Lines", TextIO.read().from(options.getInputFile()))
        .apply("Omit Empty Lines", ParDo.of(new OmitEmptyLines()))
        .apply("NLP requests", ParDo.of(new DoFn<String, String> () {
            @ProcessElement
            public void processElement(ProcessContext c) throws Exception {
                // Instantiates a client
                try (LanguageServiceClient language = LanguageServiceClient.create()) {

                  // The text to analyze
                  String text = c.element();
                  Document doc = Document.newBuilder()
                      .setContent(text).setType(Type.PLAIN_TEXT).build();

                  // Detects the sentiment of the text
                  Sentiment sentiment = language.analyzeSentiment(doc).getDocumentSentiment();                 
                  String nlp_results = String.format("Sentiment: score %s, magnitude %s", sentiment.getScore(), sentiment.getMagnitude());

                  TimeUnit.SECONDS.sleep(sleep_time.intValue()); // i.e 12s for 3 workers, n1-standard-4

                  Log.info(nlp_results);
                  c.output(nlp_results);
                }
            }
        }))
        .apply("Write Lines", TextIO.write().to(options.getOutput()));

    p.run().waitUntilFinish();
  }

  public static void main(String[] args) {
    ThrottleStepOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
      .as(ThrottleStepOptions.class);

    runThrottleStep(options);
  }
}
