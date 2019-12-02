package com.dataflow.samples;

import static com.google.common.base.MoreObjects.firstNonNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.gcp.util.Transport;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.Wait;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.common.collect.ImmutableMap;


public class DynamicBigQuerySchema {

  private static final Logger LOG = LoggerFactory.getLogger(DynamicBigQuerySchema.class);

  // public static TableSchema schema;

  public static void main(String[] args) {
    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(PipelineOptions.class);

    Pipeline p = Pipeline.create(options);

    PCollection<KV<Integer, String>> input = p
      .apply("Create data", Create.of(
            KV.of(1, "{\"user\":\"Alice\",\"age\":\"22\",\"country\":\"Denmark\"}"),
            KV.of(1, "{\"income\":\"1500\",\"blood\":\"A+\"}"),
            KV.of(1, "{\"food\":\"pineapple pizza\",\"age\":\"44\"}"),
            KV.of(1, "{\"user\":\"Bob\",\"movie\":\"Inception\",\"income\":\"1350\"}"))
      );

    PCollectionView<Map<String, String>> schemaSideInput = input  
      .apply("Build schema", ParDo.of(new DoFn<KV<Integer, String>, KV<String, String>>() {

        // A state cell holding a single Integer per key+window
        @StateId("schema")
        private final StateSpec<ValueState<Map<String, String>>> schemaSpec =
            StateSpecs.value(MapCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()));

        @ProcessElement
        public void processElement(ProcessContext c,
                                   @StateId("schema") ValueState<Map<String, String>> schemaSpec) {
          JSONObject message = new JSONObject(c.element().getValue());
          Map<String, String> current = firstNonNull(schemaSpec.read(), new HashMap<String, String>());

          message.keySet().forEach(key ->
          {
              Object value = message.get(key);

              if (!current.containsKey(key)) {
                  String type = "STRING";

                  try {
                      Integer.parseInt(value.toString());
                      type = "INTEGER";
                  }
                  catch(Exception e) {}

                  // System.out.println("key: "+ key + " value: " + value + " type: " + type);

                  c.output(KV.of(key, type));
                  current.put(key, type); 
                  schemaSpec.write(current);
              }
          });
        }
      })).apply("Save as Map", View.asMap());

    PCollectionView<Map<String, String>> schemaView = p
      .apply("Start", Create.of("Start"))
      .apply("Create Schema", ParDo.of(new DoFn<String, Map<String, String>>() {
          @ProcessElement
          public void processElement(ProcessContext c) {
            Map<String, String> schemaFields = c.sideInput(schemaSideInput);  
            List<TableFieldSchema> fields = new ArrayList<>();  
  
            for (Map.Entry<String, String> field : schemaFields.entrySet()) 
            { 
                fields.add(new TableFieldSchema().setName(field.getKey()).setType(field.getValue()));
                // LOG.info("key: "+ field.getKey() + " type: " + field.getValue());
            }

            TableSchema schema = new TableSchema().setFields(fields);

            String jsonSchema;
            try {
                jsonSchema = Transport.getJsonFactory().toString(schema);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            c.output(ImmutableMap.of("gxt-proj1:dataflow_test.dynamic_bq_schema", jsonSchema));

          }}).withSideInputs(schemaSideInput))
      .apply("Save as Singleton", View.asSingleton());

    input
      .apply("Convert to TableRow", ParDo.of(new DoFn<KV<Integer, String>, TableRow>() {
          @ProcessElement
          public void processElement(ProcessContext c) {
              JSONObject message = new JSONObject(c.element().getValue());
              TableRow row = new TableRow();

              message.keySet().forEach(key ->
              {
                  Object value = message.get(key);
                  row.set(key, value);
              });

            c.output(row);
          }}))
      .apply( BigQueryIO.writeTableRows()
          .to("gxt-proj1:dataflow_test.dynamic_bq_schema")
          .withSchemaFromView(schemaView)
          .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
          .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));

    p.run();
  }
}
