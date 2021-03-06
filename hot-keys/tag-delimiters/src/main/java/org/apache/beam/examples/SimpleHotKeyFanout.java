package org.apache.beam.examples;

import java.util.Arrays;
import java.util.List;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.Contextful;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.lang3.StringUtils;


public class SimpleHotKeyFanout {

    public static interface MyOptions extends PipelineOptions {
        @Required
        @Description("Output GCS path bucket (i.e. gs://BUCKET_NAME/data)")
        String getOutput();
        void setOutput(String s);
    }

    // our custom CombineFn implementation
    public static class TagStatisticsFn extends CombineFn<TableRow, TagStatisticsFn.Accum, String> {
        @DefaultCoder(AvroCoder.class)
        public static class Accum {
            int accepted = 0;
            int answers = 0;
            int comments = 0;
            int favorites = 0;
            int score = 0;
            int count = 0;
            long views = 0L;
        }

        @Override
        public Accum createAccumulator() { return new Accum(); }

        @Override
        public Accum addInput(Accum accum, TableRow input) {
            accum.accepted += input.get("accepted_answer_id") != null ? 1 : 0;
            accum.answers += Integer.parseInt(input.get("answer_count").toString());
            accum.comments += Integer.parseInt(input.get("comment_count").toString());
            accum.favorites += input.get("favorite_count") != null ? Integer.parseInt(input.get("favorite_count").toString()) : 0;
            accum.score += Integer.parseInt(input.get("score").toString());
            accum.views += Long.parseLong(input.get("view_count").toString());
            accum.count++;
            return accum;
        }

        @Override
        public Accum mergeAccumulators(Iterable<Accum> accums) {
            Accum merged = createAccumulator();
            for (Accum accum : accums) {
                merged.accepted += accum.accepted;
                merged.answers += accum.answers;
                merged.comments += accum.comments;
                merged.favorites += accum.favorites;
                merged.score += accum.score;
                merged.views += accum.views;
                merged.count += accum.count;
            }
            return merged;
        }

        @Override
        public String extractOutput(Accum accum) {
            return String.format(
                "Accepted ratio: %.1f%%, Avg answers: %.2f, Avg comments: %.2f, Avg stars: %.2f, Avg score: %.2f, Avg views: %.1f, Total count: %d", 
                (100* (double) accum.accepted) / accum.count, ((double) accum.answers) / accum.count, ((double) accum.comments) / accum.count,
                ((double) accum.favorites) / accum.count, ((double) accum.score) / accum.count, ((double) accum.views) / accum.count, accum.count);
        }
    }

    public static class WriteIterableFn extends SimpleFunction<KV<String, Iterable<String>>, String> {
        @Override
        public String apply(KV<String, Iterable<String>> stringIterable) {
            Iterable<String> values = stringIterable.getValue();
            StringBuilder str = new StringBuilder(); 

            for (String value:values) {
                str.append(value + "\n");
            }
            return str.toString();
        }
    }

    public static class TableRowToKV extends DoFn<TableRow, KV<String, TableRow>> {  

        private static final List<String> watchList = Arrays.asList("java", "google-cloud-dataflow", "google-bigquery", "google-cloud-storage",
              "apache-beam", "apache-beam-io", "google-cloud-platform", "google-cloud-pubsub", "spotify-scio");

        @ProcessElement
        public void processElement(ProcessContext c) {
            TableRow row = c.element();
            String rawTags = row.get("tags").toString();
            String[] tags = StringUtils.substringsBetween(rawTags, "<", ">");

            for (String tag:tags) 
            { 
                if (watchList.contains(tag)) {
                    c.output(KV.of(tag, row));
                }
            }
        }
    }

    @SuppressWarnings("serial")
    public static void main(String[] args) {
        MyOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(MyOptions.class);
        Pipeline p = Pipeline.create(options);

        String output = options.getOutput();

        p
            .apply("Read Data", BigQueryIO.read().from("bigquery-public-data:stackoverflow.posts_questions"))
            .apply("Parse + Add Keys", ParDo.of(new TableRowToKV()))
            .apply("Combine per Key", Combine.<String, TableRow, String>perKey(new TagStatisticsFn())
                .withHotKeyFanout(key -> key.equals("java") ? 10 : 1))      // "java" is the hot key
                .setCoder(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()))
            .apply("Write Files", FileIO.<KV<String, String>>write()
                .via(Contextful.fn(elem -> String.format("Tag: %s, %s", elem.getKey(), elem.getValue())), TextIO.sink())
                .withNumShards(1)
                .to(output));
        p.run();
  }
}                