## Dynamic Destinations (Java)

Quick example on how to use the [`DynamicDestinations`][1] transform to route each input element to a different output BigQuery table according to the data being processed. Originally written as an answer to a [StackOverflow question](https://stackoverflow.com/questions/56545560/how-to-get-google-dataflow-to-write-to-a-bigquery-table-name-from-the-input-data/). 

## Example

First of all, we create some dummy data where the last word will be used as the key to determine where each element should go to:

```java
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
```

With `getDestination` we control how to dispatch each element to the corresponfing table according to the key. The `getTable` method builds the fully qualified table name (prepending the prefix). We could use `getSchema` if the different tables had different schemas but that's not the case in our example so we can re-use it. Finally, we control what to write to the destination table using `withFormatFunction`:

```java
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
```

In order to run the test we create the BigQuery dataset and tables in advance:

```bash
bq mk dynamic_key
bq mk -f dynamic_key.dynamic_one Text:STRING
bq mk -f dynamic_key.dynamic_two Text:STRING
```

Then, we set the corresponding `$PROJECT`, `$BUCKET` and `$TABLE_PREFIX` (in my case `PROJECT_ID:dynamic_key.dynamic_`) variable so that we can run the job with:

```bash
mvn -Pdataflow-runner compile -e exec:java \
 -Dexec.mainClass=com.dataflow.samples.DynamicTableFromKey \
      -Dexec.args="--project=$PROJECT \
      --stagingLocation=gs://$BUCKET/staging/ \
      --tempLocation=gs://$BUCKET/temp/ \
      --output=$TABLE_PREFIX \
      --runner=DataflowRunner"
```

We can verify that each element went to the correct table:

```bash
$ bq query "SELECT * FROM dynamic_key.dynamic_one"
+---------------------------------+
|              Text               |
+---------------------------------+
| please, table one               |
| Back to one                     |
| My fave is one                  |
| this should go to table one     |
| I would like to go to table one |
+---------------------------------+

$ bq query "SELECT * FROM dynamic_key.dynamic_two"
+--------------------+
|        Text        |
+--------------------+
| I prefer table two |
| Rooting for two    |
+--------------------+
```

Tested with Java SDK 2.13.0.

  [1]: https://beam.apache.org/releases/javadoc/2.13.0/org/apache/beam/sdk/io/gcp/bigquery/DynamicDestinations.html


## License

These examples are provided under the Apache License 2.0.

## Issues

Report any issue to the GitHub issue tracker.
