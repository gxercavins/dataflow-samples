# Batch Schema auto-detect

Imagine that our input data consists of several JSON files with an arbitrary number of fields. If the BigQuery table is not created we will need to provide a schema that we might not know before analyzing the data.

Example written as an answer to a [StackOverflow question](https://stackoverflow.com/a/58809875/6121516).

## Quickstart

Set up [authentication](https://cloud.google.com/docs/authentication/) your preferred way, set the `PROJECT` and `BUCKET` variables. Then, run it locally with:

```bash
mvn compile -e exec:java \
 -Dexec.mainClass=com.dataflow.samples.DynamicBigQuerySchema \
      -Dexec.args="--project=$PROJECT \
      --tempLocation=gs://$BUCKET/temp/ \
      --runner=DirectRunner"
```

This code was tested with the Java 2.16.0 SDK and the `DirectRunner`.

## Example

We can emulate the typical auto-detect pattern: first we do a first pass through all the data to build a `Map` of all possible fields and its corresponding type (here we just considered `String` and `Integer` for readability and simplicity). We can have a [stateful][1] pipeline to keep track of the fields that have already been seen and save them as a `PCollectionView`. This way we can later use [`.withSchemaFromView()`][2] as the schema is unknown at pipeline construction. Note that this approach is only valid for batch jobs.

First, we create some dummy data without a strict schema where each row may or may not contain any of the fields:

```java
PCollection<KV<Integer, String>> input = p
  .apply("Create data", Create.of(
        KV.of(1, "{\"user\":\"Alice\",\"age\":\"22\",\"country\":\"Denmark\"}"),
        KV.of(1, "{\"income\":\"1500\",\"blood\":\"A+\"}"),
        KV.of(1, "{\"food\":\"pineapple pizza\",\"age\":\"44\"}"),
        KV.of(1, "{\"user\":\"Bob\",\"movie\":\"Inception\",\"income\":\"1350\"}"))
  );
```

We'll read the input data and build a `Map` of the different field names that we observe in the data and a really basic type checking to determine if the field contains an `INTEGER` or a `STRING`. Of course, this could be further extended if desired. Note that all data created before was assigned to the same key so that they are grouped together and we can build a complete list of fields but this could end up being a performance bottleneck. We materialize the output so that we can use it as a side input:

```java
PCollectionView<Map<String, String>> schemaSideInput = input  
  .apply("Build schema", ParDo.of(new DoFn<KV<Integer, String>, KV<String, String>>() {

    // A map containing field-type pairs
    @StateId("schema")
    private final StateSpec<ValueState<Map<String, String>>> schemaSpec =
        StateSpecs.value(MapCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()));

    @ProcessElement
    public void processElement(ProcessContext c,
                               @StateId("schema") ValueState<Map<String, String>> schemaSpec) {
      JSONObject message = new JSONObject(c.element().getValue());
      Map<String, String> current = firstNonNull(schemaSpec.read(), new HashMap<String, String>());

      // iterate through fields
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

              c.output(KV.of(key, type));
              current.put(key, type); 
              schemaSpec.write(current);
          }
      });
    }
  })).apply("Save as Map", View.asMap());
```

Now we can use the previous `Map` to build the `PCollectionView` containing the BigQuery table schema:

```java
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
        }

        TableSchema schema = new TableSchema().setFields(fields);

        String jsonSchema;
        try {
            jsonSchema = Transport.getJsonFactory().toString(schema);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        c.output(ImmutableMap.of("PROJECT_ID:DATASET_NAME.dynamic_bq_schema", jsonSchema));

      }}).withSideInputs(schemaSideInput))
  .apply("Save as Singleton", View.asSingleton());
```

Don't forget to change the fully-qualified table name `PROJECT_ID:DATASET_NAME.dynamic_bq_schema` accordingly.

Finally, in our pipeline we read the data, convert it to `TableRow` and write it to BigQuery using `.withSchemaFromView(schemaView)`:

```java
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
      .to("PROJECT_ID:DATASET_NAME.dynamic_bq_schema")
      .withSchemaFromView(schemaView)
      .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
      .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));
```

Image of the resulting BigQuery table schema, created by the pipeline:

[![enter image description here][3]][3]

and the preview of the sparse data:

[![enter image description here][4]][4]


  [1]: https://beam.apache.org/blog/2017/02/13/stateful-processing.html
  [2]: https://beam.apache.org/releases/javadoc/2.16.0/org/apache/beam/sdk/io/gcp/bigquery/BigQueryIO.Write.html#withSchemaFromView-org.apache.beam.sdk.values.PCollectionView-
  [3]: https://i.stack.imgur.com/IPbCa.png
  [4]: https://i.stack.imgur.com/MdkK8.png

## License

This example is provided under the Apache License 2.0.

## Issues

Report any issue to the GitHub issue tracker.
