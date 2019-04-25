## Extract Pub/Sub publish time

This was originally written as an answer to a [StackOverflow question](https://stackoverflow.com/questions/55370068/how-to-extract-google-pubsub-publish-time-in-apache-beam).

Sometimes we might need to access message publish time from within the pipeline but it's not present in the message element or its attributes (when using `PubsubIO.readMessagesWithAttributes()`). This is because PubsubIO, when reading the messages, will automatically assign the publish time to the element as the record timestamp. Therefore, we can access it using [`ProcessContext.timestamp()`][1]:

```java
p
	.apply("Read Messages", PubsubIO.readStrings().fromSubscription(subscription))
	.apply("Log Publish Time", ParDo.of(new DoFn<String, Void>() {
		@ProcessElement
		public void processElement(ProcessContext c) throws Exception {
			LOG.info("Message: " + c.element());
            LOG.info("Publish time: " + c.timestamp().toString()); // here
            Date date= new Date();
            Long time = date.getTime();
            LOG.info("Processing time: " + new Instant(time).toString());
		}
	}));
```

To test this, we can publish a message a little bit ahead to have a significant difference between event and processing time. Resulting output with DirectRunner (note the 6 min difference):

```java
Mar 27, 2019 11:03:08 AM com.dataflow.samples.LogPublishTime$1 processElement
INFO: Message: I published this message a little bit before
Mar 27, 2019 11:03:08 AM com.dataflow.samples.LogPublishTime$1 processElement
INFO: Publish time: 2019-03-27T09:57:07.005Z
Mar 27, 2019 11:03:08 AM com.dataflow.samples.LogPublishTime$1 processElement
INFO: Processing time: 2019-03-27T10:03:08.229Z
```

Full code can be found in the `LogPublishTime.java` file.


  [1]: https://beam.apache.org/releases/javadoc/2.0.0/index.html?org/apache/beam/sdk/transforms/DoFn.ProcessContext.html

## License

These examples are provided under the Apache License 2.0.

## Issues

Report any issue to the GitHub issue tracker.
