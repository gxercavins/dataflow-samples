package com.dataflow.samples;

import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.DynamicDestinations;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.ValueInSingleWindow;

import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.bigquery.model.TableFieldSchema;

import java.util.ArrayList;
import java.util.List;


public abstract class DynamicTableFromKey {

    public interface Options extends PipelineOptions {
        @Description("Output BigQuery Table Prefix <project_id>:<dataset>.<table_prefix>")
        String getOutput();

        void setOutput(String s);
    }

    public static void main(String[] args) {

        DynamicTableFromKey.Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(DynamicTableFromKey.Options.class);

        Pipeline p = Pipeline.create(options);

        String output = options.getOutput();

        p.apply("Create Data", Create.of("this should go to table one",
                                         "I would like to go to table one",
                                         "please, table one",
                                         "I prefer table two",
                                         "Back to one",
                                         "My fave is one",
                                         "Rooting for two"))
        .apply("Create Keys", ParDo.of(new DoFn<String, KV<String,String>>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
              String[] splitBySpaces = c.element().split(" ");
              c.output(KV.of(splitBySpaces[splitBySpaces.length - 1],c.element()));
            }
          }))
        .apply(BigQueryIO.<KV<String, String>>write()
        .to(new DynamicDestinations<KV<String, String>, String>() {
            public String getDestination(ValueInSingleWindow<KV<String, String>> element) {
                return element.getValue().getKey();
            }
            public TableDestination getTable(String name) {
              String tableSpec = output + name;
                return new TableDestination(tableSpec, "Table for type " + name);
            }
            public TableSchema getSchema(String schema) {
                  List<TableFieldSchema> fields = new ArrayList<>();

                fields.add(new TableFieldSchema().setName("Text").setType("STRING"));
              TableSchema ts = new TableSchema();
              ts.setFields(fields);
              return ts;
            }
        })
          .withFormatFunction(new SerializableFunction<KV<String, String>, TableRow>() {
            public TableRow apply(KV<String, String> row) {
                TableRow tr = new TableRow();
            
                    tr.set("Text", row.getValue());
            return tr;
            }
        })
        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));

        p.run();

    }
}