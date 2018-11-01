## When are Pub/Sub Messages ACKed?

A common Pub/Sub workflow involves reading some messages from a subscription and doing some operations with it. Once and only once the message is successfully processed, the subscriber acknowledges it and it is removed from the subscription backlog. For instance, in this minimal Python snippet from the [quickstart](https://cloud.google.com/pubsub/docs/quickstart-client-libraries#pubsub-client-libraries-python) where it just prints the received messages and ACKs them:

```python
def callback(message):
    print('Received message: {}'.format(message))
    message.ack()
```

With Dataflow it is convenient to use the [`PubsubIO`](https://beam.apache.org/releases/javadoc/2.5.0/index.html?org/apache/beam/sdk/io/gcp/pubsub/PubsubIO.html) connector instead of sending requests to the Pub/Sub API using a Java library. However, we lose direct control over when are messages ACKed. `PubsubIO` uses [`PubsubUnboundedSource`](https://github.com/apache/beam/blob/v2.5.0/sdks/java/io/google-cloud-platform/src/main/java/org/apache/beam/sdk/io/gcp/pubsub/PubsubUnboundedSource.java) under the hood and, the checkpointing section [reads](https://github.com/apache/beam/blob/v2.5.0/sdks/java/io/google-cloud-platform/src/main/java/org/apache/beam/sdk/io/gcp/pubsub/PubsubUnboundedSource.java#L227):

> Which messages have been durably committed and thus can now be ACKed.

But what does this mean in practice?

Messages will be considered committed, and therefore ACKed, after the first FUSED step. This will depend on how the actual pipeline execution graph is constructed as, due to optimization, different steps can be fused together and executed sequentially in the same worker as explained [here](https://cloud.google.com/dataflow/service/dataflow-service-desc#fusion-optimization).

In this example we'll play with two pipelines. The first one will read from Pub/Sub and write to Pub/Sub, consisting of a single fused step. Messages that are not written to the sink will NOT be ACKed and remain in the subscription backlog. For the second one, we'll read once more from Pub/Sub but write to BigQuery instead. This pipeline will consist of different fused steps so there can be messages not successfully inserted into the destination table which are already committed/ACKed. We'll also explore how to prevent data loss in those situations.

## Before you start

Set up [authentication](https://cloud.google.com/docs/authentication/) your preferred way and define the `$PROJECT` and `$BUCKET` variables. This code was tested with Java SDK 2.5.0.

Throughout this example we will publish messages manually in order to have better control of what is going on. Also, keep in mind that `PubsubIO` can read from either topics or subscriptions. If we choose the former and read directly from the topic a temporary subscription will be created. As we are interested in checkpointing and being able to monitor what is in the backlog we want to read from subscriptions instead and use `gcloud` commands in the Cloud SDK to pull messages.

Pub/Sub topics and subscriptions used:
* Input topic: `$ gcloud pubsub topics create input-topic`
* Input subscription: `$ gcloud pubsub subscriptions create input-sub --topic input-topic`
* Output topic: `$ gcloud pubsub topics create output-topic`
* Output subscription: `$ gcloud pubsub subscriptions create output-sub --topic output-topic`

BigQuery tables used:
* Create dataset: `$ bq mk pubsub_ack`
* Create table one: `$ bq mk --table pubsub_ack.table1 EventTime:STRING,Message:STRING`
* Create table two: `$ bq mk --table pubsub_ack.table2 EventTime:STRING,Message:STRING`

## Example

First, we'll use `FusedPipeline.java`. We use `PubsubIO` to read messages from the input subscription (as explained in the previous section):

```java
.apply("Read Messages", PubsubIO.readStrings().fromSubscription(subscription))
```

and in the dummy processing step we'll just throw an unhandled exception when the message is less than 5 characters long:

```java
.apply("Process Messages", ParDo.of(new DoFn<String, String>() {
    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      if (c.element().length() >= 5) {
        c.output(c.element());
      }
      else {
        throw new Exception();
      }
  }
}))  
```

The main idea is to see what will happen with those short messages. They will be read successfully but will fail during processing. Finally, we write them to a different topic:

```java
.apply("Write Messages", PubsubIO.writeStrings().to(output));
```

Then, we can run the pipeline with (replace variables if needed):

```bash
mvn compile -e exec:java \
 -Dexec.mainClass=com.dataflow.samples.FusedPipeline \
      -Dexec.args="--project=$PROJECT \
      --stagingLocation=gs://$BUCKET/staging/ \
      --tempLocation=gs://$BUCKET/staging/ \
      --input=projects/$PROJECT/subscriptions/input-sub \
      --output=projects/$PROJECT/topics/output-topic \
      --runner=DataflowRunner"
```

If we inspect the job through the [Console UI](https://console.cloud.google.com/dataflow) and click on the `Logs` button we'll see that there is a single large fused step:

>  Executing operation Read Messages/PubsubUnboundedSource+Read Messages/MapElements/Map+Process Messages+Write Messages/MapElements/Map+Write Messages/PubsubUnboundedSink

![img1](https://user-images.githubusercontent.com/29493411/47264907-b4dec180-d51f-11e8-961c-f693ed2ce2e5.png)

All the operations are part of a single step and the message will not be ACKed until it's written to the sink. Once the Dataflow workers have been initialized, we can proceed to publish two messages that will be successfully processed:
* `$ gcloud pubsub topics publish input-topic --message="message one"`
* `$ gcloud pubsub topics publish input-topic --message="message two"`

Once the elements exit the pipeline we can pull the messages in the output subscription and we should see something like this:

```bash
$ gcloud pubsub subscriptions pull output-sub --limit 10 --auto-ack
┌─────────────┬─────────────────┬────────────┐
│     DATA    │    MESSAGE_ID   │ ATTRIBUTES │
├─────────────┼─────────────────┼────────────┤
│ message two │ 271522081637846 │            │
│ message one │ 271522081443427 │            │
└─────────────┴─────────────────┴────────────┘
```

The messages have been durably committed to the sink so they have been ACKed and no longer remain on the input subscription's backlog:

```bash
$ gcloud pubsub subscriptions pull input-sub --limit 10 --auto-ack
Listed 0 items.
```

Now, however, we'll publish a shorter message that will throw an exception within the filter ParDo:

```bash
$ gcloud pubsub topics publish input-topic --message="test"
```

![img2](https://user-images.githubusercontent.com/29493411/47264908-b4dec180-d51f-11e8-8eb0-d1d42aff5014.png)

The message will not be committed to the sink topic:

```bash
$ gcloud pubsub subscriptions pull output-sub --limit 10 --auto-ack
Listed 0 items.
```

And, as it's still UNACKed from the input subscription, we can pull it manually and see that it was still in the backlog:

```bash
gcloud pubsub subscriptions pull input-sub --limit 10 --auto-ack
┌──────┬─────────────────┬────────────┐
│ DATA │    MESSAGE_ID   │ ATTRIBUTES │
├──────┼─────────────────┼────────────┤
│ test │ 223288442531285 │            │
└──────┴─────────────────┴────────────┘
```

Therefore, in these cases where we have a single fused step, messages that are not fully processed still remain in the backlog and there is no risk of data loss. What happens, however, when we have several steps?

To demonstrate that use case we can run the code in either `NotFusedPipeline.java` or `TwoNotFusedOutputs.java`. Images correspond to the latter but, for simplicity, we'll take a look at the single-sink one. We read and process messages the same way than before with the difference that now we just filter out short messages instead of raising an exception. Now, as we are streaming into a BigQuery destination table we'll need to convert messages to `TableRow` format. We'll write the element timestamp and content to one of the previously created tables.

```java
// convert message to TableRow
.apply("Convert to TableRow", ParDo.of(new DoFn<String, TableRow>() {
    @ProcessElement
    public void processElement(ProcessContext c) {
      TableRow row = new TableRow();
      row.set("EventTime", c.timestamp().toString());              
      row.set("Message", c.element());
      c.output(row);
  }
}))

// write messages to BigQuery destination table
.apply("Write Messages", BigQueryIO.writeTableRows().to(output)
    .withSchema(schema)
    .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
    .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER));
```

We can run it with:
```bash
mvn compile -e exec:java \
 -Dexec.mainClass=com.dataflow.samples.NotFusedPipeline \
      -Dexec.args="--project=$PROJECT \
      --stagingLocation=gs://$BUCKET/staging/ \
      --tempLocation=gs://$BUCKET/staging/ \
      --input=projects/$PROJECT/subscriptions/input-sub \
      --output=$PROJECT:pubsub_ack.table1 \
      --runner=DataflowRunner"
```

If you prefer to tun the dual-sink example use instead main class `com.dataflow.samples.TwoNotFusedOutputs` and `--output1=$PROJECT:pubsub_ack.table1`, `--output2=$PROJECT:pubsub_ack.table2` output options.

Now the pipeline will contain more than a single fused step (even if one of them will be relatively large). Therefore, messages will be considered committed before actually writing them to the destination sink:

![img3](https://user-images.githubusercontent.com/29493411/47254474-7388dc00-d463-11e8-8da9-0e6908f4b6ff.png)

We can publish some messages (one to four) as we did before to double check that the pipelines behaves as expected:

![img4](https://user-images.githubusercontent.com/29493411/47254475-7388dc00-d463-11e8-9af9-8d92da607210.png)

And now we'll delete one of the tables and publish a fifth message:

```bash
bq rm -f -t pubsub_ack.table2
gcloud pubsub topics publish input-topic --message="message five"
```

As the table no longer exists we'll get a `404 Not Found` error:

![img5](https://user-images.githubusercontent.com/29493411/47254476-7388dc00-d463-11e8-9f59-8a2b5d5021d7.png)

The message, of course, is not written to the destination table but has already been ACKed:

```bash
$ gcloud pubsub subscriptions pull input-sub --limit 10 --auto-ack
Listed 0 items.
```

Is the message lost in this case? Data is durably committed within the pipeline so unless we cancel or drain the job we can still recover it. In this example we can just re-create the table again. For other errors, such as schema mismatch, to "rescue" in-flight data we can resort to a job update (it might not be feasible for all cases, though, and will depend on which step is the data buffered). Refer to the [docs](https://cloud.google.com/dataflow/pipelines/updating-a-pipeline) for more information. Launch the job with the same name of the pipeline that you want to update in the `--jobName` parameter and add the `--update` flag. Both pipelines will have the same job name. If you inspect the stuck job, in the job summary tab you'll see the ID of the new updated job where processing will continue.

## Conclusions

As a summary, the way this is implemented in Dataflow, we don't have direct control on when messages are ACKed. This will depend on the underlying structure of the pipeline but it's not straightforward to force/prevent fusion. In most cases, we'll have more than a single fused step and need to account for updating streaming jobs instead of cancelling/draining them to avoid data loss.

If we have more stringent requirements we can consider using `com.google.api.services.pubsub` and `com.google.api.client.http` libraries. Examples (publishing, not reading messages, but can be a good start) can be found [here](https://github.com/GoogleCloudPlatform/cloud-pubsub-samples-java/tree/master/dataflow/src/main/java/com/google/cloud/dataflow/examples).

## Shutdown

Streaming jobs will run until we issue the drain/cancel command. We can do so using the UI, the API, Client libraries or the `gcloud` command, for example (you might need to account for the `region` if specified at runtime):

```bash
gcloud dataflow jobs cancel $(gcloud dataflow jobs list | grep -m 1 fusedpipeline | cut -f 1 -d " ")
gcloud dataflow jobs cancel $(gcloud dataflow jobs list | grep -m 1 notfusedpipeline | cut -f 1 -d " ")
gcloud dataflow jobs cancel $(gcloud dataflow jobs list | grep -m 1 twonotfused | cut -f 1 -d " ")
```

Delete the BigQuery tables:

```bash
bq rm -f -t pubsub_ack.table1
bq rm -f -t pubsub_ack.table2
```

and Pub/Sub topics/subscriptions:

```bash
gcloud pubsub subscriptions delete input-sub
gcloud pubsub topics delete input-topic
gcloud pubsub subscriptions delete output-sub
gcloud pubsub topics delete output-topic
```

## License

These examples are provided under the Apache License 2.0.

## Issues

Report any issue to the GitHub issue tracker.
