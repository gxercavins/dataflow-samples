package org.apache.beam.examples;

import java.util.Arrays;
import java.util.List;

import com.google.api.services.bigquery.model.TableRow;
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
import org.apache.beam.sdk.transforms.Contextful;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;


public class ChangeIngestion {

    public static interface MyOptions extends PipelineOptions {
        @Required
        @Description("Output GCS path bucket (i.e. gs://BUCKET_NAME/data)")
        String getOutput();
        void setOutput(String s);
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

    public static class TableRowToKV extends DoFn<TableRow, KV<String, String>> {  
        @ProcessElement
        public void processElement(ProcessContext c) {
            TableRow row = c.element();
            c.output(KV.of(row.get("tag").toString(), row.get("title").toString()));
        }
    }

    @SuppressWarnings("serial")
    public static void main(String[] args) {
        MyOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(MyOptions.class);
        Pipeline p = Pipeline.create(options);

        String output = options.getOutput();

        String query = "SELECT tag, title\n"
                     + "FROM `bigquery-public-data.stackoverflow.posts_questions` AS questions\n"
                     + "CROSS JOIN UNNEST(split(questions.tags,'|')) AS tag\n"
                     + "WHERE tag IN ('java', 'google-cloud-dataflow', 'google-bigquery', 'google-cloud-storage',\n"
                     + "              'apache-beam', 'apache-beam-io', 'google-cloud-platform', 'google-cloud-pubsub', 'spotify-scio')";

        p
            .apply("Read Data", BigQueryIO.read().fromQuery(query).usingStandardSql())
            .apply("Parse + Add Keys", ParDo.of(new TableRowToKV()))
            .apply("Group By Key", GroupByKey.<String, String>create())
            .apply("Write Files", FileIO.<String, KV<String, Iterable<String>>>writeDynamic()
                .by(elem -> elem.getKey())
                .withDestinationCoder(StringUtf8Coder.of())
                .via(Contextful.fn(new WriteIterableFn()), TextIO.sink())
                .to(output)
                .withNaming(key -> FileIO.Write.defaultNaming("tag-" + key, ".txt")));

        p.run();
  }
}                