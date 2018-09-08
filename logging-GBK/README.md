# Logging GBK

This example uses a logging function that can be used to examine useful information after grouping data by key such as the window, pane timing, key, number of values, event time, processing time, etc. This will be logged into Stackdriver Logging and/or written to a BigQuery destination table.

## Quickstart

You can use the provided `run.sh` script (don't forget to add execution permissions `chmod +x run.sh`) as in:
``` bash
./run.sh <DATAFLOW_PROJECT_ID> <BUCKET_NAME> <PUB/SUB_INPUT_TOPIC> <BIGQUERY_PROJECT_ID:DATASET.TABLE>
```

Alternatively, follow these steps:
* Set up [authentication](https://cloud.google.com/docs/authentication/) your preferred way 
* Set the `$PROJECT`, `$BUCKET`, `$TOPIC` and `$TABLE` variables and run the Dataflow job:
``` bash
mvn compile -e exec:java \
 -Dexec.mainClass=com.dataflow.samples.LogGBK \
      -Dexec.args="--project=$PROJECT \
      --stagingLocation=gs://$BUCKET/staging/ \
      --tempLocation=gs://$BUCKET/staging/ \
      --input=projects/$PROJECT/topics/$TOPIC \
      --output=$TABLE \
      --runner=DataflowRunner"
```
* To disable either Stackdriver Logging or BigQuery use `--stackdriver=false` or `--bigquery=false`.
* If you need dummy data you can use the `publish.py` example, adapted from the [quickstart](https://cloud.google.com/pubsub/docs/quickstart-client-libraries#pubsub-client-libraries-python)
* Install the `google-cloud-pubsub` Python package: `pip install google-cloud-pubsub` (use of `virtualenv` is recommended)
* Run the `publish.py` script to start publishing messages: `python publish.py $PROJECT publish $TOPIC`

This code was tested with Java SDK 2.5.0.

## Example

Two tags will be created to differentiate between the main output and the side output that writes to BigQuery. Declared as `static` as they will be accessed from within a ParDo outside of the main function:

```java
public static final TupleTag<KV<String,Iterable<String>>> mainOutputTag = new TupleTag<KV<String,Iterable<String>>>(){};
public static final TupleTag<TableRow> logToBigQueryTag = new TupleTag<TableRow>(){};
```

The `LoggingFn` ParDo is shown below. Data, pane and timestamp information are retrieved from [`DoFn.ProcessContext`](https://beam.apache.org/documentation/sdks/javadoc/2.5.0/org/apache/beam/sdk/transforms/DoFn.ProcessContext.html) while upper and lower window bounds are returned by [`BoundedWindow`](https://beam.apache.org/documentation/sdks/javadoc/2.5.0/org/apache/beam/sdk/transforms/windowing/BoundedWindow.html). In case data to be logged can contain sensitive information keys are hashed and only the number of values per key and window is saved. Side output (`logToBigQueryTag`) contains `TableRow` elements so that they can be written to BigQuery using `BigQueryIO`. Input elements pass-through the function to the main output (`mainOutputTag`) without modifications.

```java
static class LoggingFn extends DoFn<KV<String,Iterable<String>>,KV<String,Iterable<String>>> {
    // access options to enable/disable Stackdriver Logging and BigQuery
    private final Boolean stackdriver;
    private final Boolean bigquery;
    public LoggingFn(Boolean stackdriver, Boolean bigquery) {
        this.stackdriver = stackdriver;
        this.bigquery = bigquery;
    }
    @ProcessElement
    public void processElement(ProcessContext c, BoundedWindow window, MultiOutputReceiver r) {
        TableRow row = new TableRow();
        PaneInfo pane = c.pane();
        Integer key = c.element().getKey().hashCode();    // hashed for data privacy
        Iterable<String> values = c.element().getValue();
        Date date= new Date();
        Long time = date.getTime();
        String processingTime = new Instant(time).toString();
        String eventTime = c.timestamp().toString();
        
        // if enabled, log entries to Stackdriver Logging
        if (stackdriver) {
            String logString = String.format("key=%s, numElements=%d, window=%s, Pane: [isFirst=%s, isLast=%s, timing=%s], eventTime=%s, processingTime=%s", key, Iterators.size(values.iterator()), window.toString(), pane.isFirst(), pane.isLast(), pane.getTiming(), eventTime, processingTime);
            LOG.info(logString);
        }

        // if enabled, divert log entries to BigQuery using a side output
        if (bigquery) {
            row.set("Key", key);
            row.set("NumberOfElements", Iterators.size(values.iterator()));
            row.set("Window", window.toString());              
            row.set("PaneIsFirst", pane.isFirst());
            row.set("PaneIsLast", pane.isLast());
            row.set("PaneTiming", pane.getTiming());
            row.set("EventTime", eventTime);
            row.set("ProcessingTime", processingTime);

            r.get(logToBigQueryTag).output(row);
        }
        
        r.get(mainOutputTag).output(c.element());
    }
}
```

Specifying `ParDo.withOutputTags(mainOutputTag, TupleTagList.of(logToBigQueryTag)))` we indicate the main and side outputs used by the ParDo. Then, those will be received by [`DoFn.MultiOutputReceiver`](https://beam.apache.org/documentation/sdks/javadoc/2.5.0/org/apache/beam/sdk/transforms/DoFn.MultiOutputReceiver.html).

```java
.apply("Log Info", ParDo.of(new LoggingFn(stackdriver, bigquery))
            .withOutputTags(mainOutputTag, TupleTagList.of(logToBigQueryTag)));
```

The full pipeline demonstrates a use case of this logging function. It reads elements from a Pub/Sub topic and assigns it a key, which will be the first word of the message, with the following DoFn:

```java
.apply("Create Keys", ParDo.of(new DoFn<String, KV<String,String>>() {
    @ProcessElement
    public void processElement(ProcessContext c, BoundedWindow window) {
      c.output(KV.of(c.element().split(" ")[0],c.element()));
  }
}))
```

Then, we apply the Group By Key and, after that, the logging function on the grouped data. We can call `results.get(<TAG>)` to indicate which output are we processing. Main pipeline execution can resume afterwards with:

```java
results.get(mainOutputTag)
    .apply("Continue Processing", ParDo.of(new DoFn<KV<String,Iterable<String>>, Void>() {
        @ProcessElement
        public void processElement(ProcessContext c) {
            LOG.info("New element in main output");
            // do something
            // ...
    }})); 
```

Side output results will be streamed, if enabled, into BigQuery:
```java
results.get(logToBigQueryTag)
    .apply("Write Logs", BigQueryIO.writeTableRows().to(output)
        .withSchema(schema)
        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));
```

We can see the logs added by the `Log Info` step and how output is divided into main and side output collections:

![screenshot from 2018-09-08 19-01-14](https://user-images.githubusercontent.com/29493411/45256658-e5b4ce00-b399-11e8-86a6-cecf680c284a.png)

Sample BigQuery writes through tusing the provided `publish.py` script:

![screenshot from 2018-09-03 21-27-35](https://user-images.githubusercontent.com/29493411/44999848-8bde8d80-afc0-11e8-8e65-0be3c62dc8ec.png)

If we disable Stackdriver and BigQuery logging, the step will not produce additional logging and no elements will be written to BigQuery (elements only added to main output collection):

![screenshot from 2018-09-08 17-51-04](https://user-images.githubusercontent.com/29493411/45256182-29580980-b393-11e8-8595-06641c447e37.png)

Another possibility is to set the worker log level to `TRACE` as this will provide additional information (for example, related to windowing) but can cause excessive logging to happen:

```java
DataflowWorkerLoggingOptions loggingOptions = options.as(DataflowWorkerLoggingOptions.class);
loggingOptions.setDefaultWorkerLogLevel(Level.TRACE);
```

## Next Steps

* If possible, use generic types for expanded compatibility

## License

These examples are provided under the Apache License 2.0.

## Issues

Report any issue to the GitHub issue tracker.
