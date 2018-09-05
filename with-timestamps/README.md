# WithTimestamps Example

This example will use [`WithTimestamps`](https://beam.apache.org/documentation/sdks/javadoc/2.5.0/org/apache/beam/sdk/transforms/WithTimestamps.html) to assign timestamps to elements according to processing time instead of event time (timestamps assigned when published to Pub/Sub). Also, we'll see how to produce data that it's already behind the watermark by tampering with the timestamps. However, take into account, that ideally lateness should be introduced by the data source.

## Quickstart

You can use the provided `run.sh` script (don't forget to add execution permissions `chmod +x run.sh`) as in:
``` bash
./run.sh <DATAFLOW_PROJECT_ID> <BUCKET_NAME> <PUB/SUB_INPUT_TOPIC> <BIGQUERY_PROJECT_ID:DATASET.TABLE>
```

This will also publish 4 messages to the specified Pub/Sub topic.

Alternatively, follow these steps:
* Set up [authentication](https://cloud.google.com/docs/authentication/) your preferred way 
* Set the `$PROJECT`, `$BUCKET`, `$TOPIC` and `$TABLE` variables and run the Dataflow job:
``` bash
mvn compile -e exec:java \
 -Dexec.mainClass=com.dataflow.samples.LateData \
      -Dexec.args="--project=$PROJECT \
      --stagingLocation=gs://$BUCKET/staging/ \
      --tempLocation=gs://$BUCKET/staging/ \
      --input=projects/$PROJECT/topics/$TOPIC \
      --output=$TABLE \
      --runner=DataflowRunner"
```

This code was tested with Java SDK 2.5.0.

## Example

We'll define a `timestampFn` serializable function whose output will depend on the data. It will take as input a Pub/Sub message parsed to String and split it into words. If the first one is `"late"` then the function will assign the current instant minus two minutes. Otherwise, it will just output the processing instant.

```java
input -> input.split(" ")[0].toString().equals("late") ? new Instant(new DateTime().minus(Duration.standardMinutes(2))) : new Instant(new DateTime());
```

In order to be able to do so and assign timestamps which belong to past elements that are already behind the watermark we need to modify [`withAllowedTimestampSkew`](https://beam.apache.org/documentation/sdks/javadoc/2.5.0/org/apache/beam/sdk/transforms/WithTimestamps.html#withAllowedTimestampSkew-org.joda.time.Duration-). By default this is set to `Duration.ZERO` and only allows data to be "shifted" to the future. For our use case, we'll set it to the maximum allowed value `Long.MAX_VALUE`:

```java
.apply("Timestamps", WithTimestamps.of(timestampFn).withAllowedTimestampSkew(new Duration(Long.MAX_VALUE)))
```

Again, the recommended way is that late data should be produced by the source itself and that's why it's deprecated. However, it might be interesting enough to use it anyway.

If we write each message and timestamp to a BigQuery destination table we can confirm that timestamps are assigned as expected. If we use the examples in `run.sh`, publish them all at approximately the same time, the results will show a two-minute early message:

> late data

![screenshot from 2018-09-05 19-57-24](https://user-images.githubusercontent.com/29493411/45114328-03d9be80-b14d-11e8-9ac8-debe07c77e0c.png)

## Shutdown

As a streaming job this will run until we issue the drain/cancel command. You can do so using the UI, the API, Client libraries or the `gcloud` command, for example (you might need to account for the `region` if specified at runtime):
``` bash
gcloud dataflow jobs cancel $(gcloud dataflow jobs list | grep -m 1 latedata | cut -f 1 -d " ")
```

## License

These examples are provided under the Apache License 2.0.

## Issues

Report any issue to the GitHub issue tracker.
