# Get Pub/Sub attribute

How to get an attribute for a Pub/Sub message within a Dataflow pipeline using `PubsubIO`. This example was originally written a a test for this [StackOverflow question](https://stackoverflow.com/questions/59219937/getattribute-from-pubsubmessage-in-dataflow).

## Quickstart

Run locally with:

```bash
mvn compile -e exec:java \
 -Dexec.mainClass=com.dataflow.samples.PubSubGetAttribute \
      -Dexec.args="--input=projects/$PROJECT/topics/$TOPIC \
      --runner=DirectRunner"
```

And on Dataflow with:

```bash
mvn -Pdataflow-runner compile -e exec:java \
 -Dexec.mainClass=com.dataflow.samples.PubSubGetAttribute \
      -Dexec.args="--input=projects/$PROJECT/topics/$TOPIC \
      --project=$PROJECT \
      --tempLocation=gs://$BUCKET/temp \
      --stagingLocation=gs://$BUCKET/staging \
      --runner=DataflowRunner"
```

Replace the variables as needed. Tested with Java 2.16.0 SDK.

## Example

We can use `PubsubIO.readMessagesWithAttributes()` to read messages into a `PubsubMessage` Object. Then, we can access a particular attribute with `getAttribute("attribute")`:

```java
p
	.apply("Read Messages", PubsubIO.readMessagesWithAttributes().fromTopic(topic))
	.apply("Log Event ID", ParDo.of(new DoFn<PubsubMessage, String>() {
		@ProcessElement
		public void processElement(ProcessContext c) throws Exception {
			try {
				String event = c.element().getAttribute("evId");
				LOG.info("Event ID: " + event);
				c.output(event);
			}
			catch(Exception e) { }
		}
	}));
```

We publish one message:

```bash
$ gcloud pubsub topics publish $TOPIC --message Hi --attribute evId=1234
```

And get the correct value for the specified attribute:

```java
INFO: Event ID: 1234
```

## License

These examples are provided under the Apache License 2.0.

## Issues

Report any issue to the GitHub issue tracker.
