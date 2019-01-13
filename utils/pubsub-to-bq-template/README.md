# Pub/Sub to BigQuery template

This example tweaks the [official template](https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/master/src/main/java/com/google/cloud/teleport/templates/PubSubToBigQuery.java) to use PubSubIO's `fromSubscription` method instead of `fromTopic` ([docs](https://beam.apache.org/releases/javadoc/2.8.0/org/apache/beam/sdk/io/gcp/pubsub/PubsubIO.Read.html)).

Main motivation is that, when using `fromTopic` it creates some temporary subscriptions. Messages published to that topic before or after the job is running are lost. If, instead, `fromSubscription` is used unacked messages are accumulated in the backlog and it's possible to stop the Dataflow pipeline and resume it when needed.

Needless to say this is an **UNOFFICIAL** example.

## Quickstart

Set up [authentication](https://cloud.google.com/docs/authentication/) your preferred way.

If you want to edit the code and create your own template, downloaded the official repo:

```bash
git clone https://github.com/GoogleCloudPlatform/DataflowTemplates.git
```

and modify `DataflowTemplates/src/main/java/com/google/cloud/teleport/templates/PubSubToBigQuery.java` with the `PubSubToBigQuery.java` file here.

Set up the corresponding `PROJECT` and `BUCKET`variables. Then, stage the template with:
```bash
mvn compile exec:java \
 -Dexec.mainClass=com.google.cloud.teleport.templates.PubSubToBigQuery \
 -Dexec.cleanupDaemonThreads=false \
 -Dexec.args=" \
 --project=$PROJECT \
 --stagingLocation=gs://$BUCKET/staging \
 --tempLocation=gs://$BUCKET/temp \
 --templateLocation=gs://$BUCKET/templates/PubSubToBigQuery \
 --runner=DataflowRunner"
```

Now you'll have your own version of the template that you can execute with:
```bash
gcloud dataflow jobs run pubsub-to-bigquery \
  --project=$PROJECT \
  --gcs-location=gs://$BUCKET/templates/PubSubToBigQuery \
  --parameters=inputSubscription=projects/$PROJECT/subscriptions/$SUB_NAME,outputTableSpec=$BQ_TABLE,outputDeadletterTable=${BQ_TABLE}_deadletter
```

Be sure to set up `BQ_TABLE` and `SUB_NAME` beforehand. If needed create the BigQuery table, for example:
```bash
bq mk --table $BQ_TABLE k1:STRING,k2:STRING
```

To test that it works just publish any message with JSON format to the Pub/Sub topic (`TOPIC_NAME`) associated with that subscription.
```
gcloud pubsub topics publish $TOPIC_NAME --message='{k1:"v1", k2:"v2"}'
```

You can, instead, just copy the template given here (`PubSubToBigQuery` file) or just invoke my staged template by replacing `--gcs-location` for `gs://my-dataflow-templates/PubSubToBigQuery`.

This code was tested with version 2.8.0 of the Java SDK.

## Example

This is a very simple change. Both methods are directly interchangeable so we can replace:
```java
PubsubIO.readMessagesWithAttributes().fromTopic(options.getInputTopic()))
```

for:
```java
PubsubIO.readMessagesWithAttributes().fromSubscription(options.getInputSubscription()))
```

Notice that, for consistency reasons, we'll also change the option so that the parameter is `--inputSubscription`. The new getter and setter methods for the ValueProvider will be:
```java
@Description("Pub/Sub subscription to read the input from")
ValueProvider<String> getInputSubscription();

void setInputSubscription(ValueProvider<String> value);
```

Also the example on top with instructions on how to run it has been updated accordingly.

## License

These examples are provided under the Apache License 2.0.

## Issues

Report any issue to the GitHub issue tracker.
