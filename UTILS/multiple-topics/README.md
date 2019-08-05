## Read from multiple Pub/Sub topics

We want to pass a list of comma-separated Pub/Sub topics as a `--topicList` option parameter and apply the same steps to each source in parallel. Note that this approach has its issues if we want to stage it as a template and the number of topics can be variable, as explained in this StackOverflow [answer](https://stackoverflow.com/a/57064537/6121516).

## Example

We can read the `--topicList` topics and build the different streams with a `for` loop as:

```java
String[] listOfTopicStr = options.getTopicList().split(",");

PCollection[] p = new PCollection[listOfTopicStr.length];

for (int i = 0; i < listOfTopicStr.length; i++) {
    p[i] = pipeline
        .apply(PubsubIO.readStrings().fromTopic(listOfTopicStr[i]))
        .apply(ParDo.of(new DoFn<String, Void>() {
            @ProcessElement
            public void processElement(ProcessContext c) throws Exception {
                Log.info(String.format("Message=%s", c.element()));
            }
        }));
}
```

A quick way to test the pipeline with 3 input topics:

```bash
mvn -Pdataflow-runner compile -e exec:java \
 -Dexec.mainClass=com.dataflow.samples.MultipleTopics \
      -Dexec.args="--project=$PROJECT \
      --topicList=projects/$PROJECT/topics/topic1,projects/$PROJECT/topics/topic2,projects/$PROJECT/topics/topic3 \
      --stagingLocation=gs://$BUCKET/staging/ \
      --runner=DataflowRunner"

gcloud pubsub topics publish topic1 --message="message 1"
gcloud pubsub topics publish topic2 --message="message 2"
gcloud pubsub topics publish topic3 --message="message 3"
```

The output and Dataflow graph will be as expected:

[![enter image description here][1]][1]

This code was tested with the 2.12.0 SDK.

  [1]: https://i.stack.imgur.com/vP7op.png

## License

These examples are provided under the Apache License 2.0.

## Issues

Report any issue to the GitHub issue tracker.
