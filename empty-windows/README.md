# Empty windows
## Inform/alert about data rate even when there is no input data

The main idea for this example is to have a constant monitoring/alerting system that logs the data rate or volume of incoming data. The problem comes for empty windows that do not process any element and, therefore, do not trigger or emit any pane.

This was originally written as an answer to [this StackOverflow question](https://stackoverflow.com/questions/54503190/live-monitoring-using-apache-beam/54543527#54543527).

## Quickstart

You can use the provided `run.sh` script as in:
``` bash
./run.sh <DATAFLOW_PROJECT_ID> <BUCKET_NAME> <PUB/SUB_INPUT_TOPIC>
```

Alternatively, follow these steps:
* Set up [authentication](https://cloud.google.com/docs/authentication/) your preferred way 
* Set the `$PROJECT`, `$BUCKET` and `$TOPIC` variables and run the Dataflow job:
``` bash
mvn compile -e exec:java \
 -Dexec.mainClass=com.dataflow.samples.EmptyWindows \
      -Dexec.args="--project=$PROJECT \
      --stagingLocation=gs://$BUCKET/staging/ \
      --tempLocation=gs://$BUCKET/staging/ \
      --input=projects/$PROJECT/topics/$TOPIC \
      --runner=DataflowRunner"
```
This code was tested with Java SDK 2.5.0.

## Example

Beam runners do not emit panes for empty windows. In order to do so they would need stateful windowing assignment and automatic inference of missing windows. This, combined with the triggering strategy and current watermark, would allow to emit result for the empty window that should have been closed. In addition, downstream pipeline stages should be able to handle those empty panes. 

There are, however, a couple workarounds to achieve the intended behavior -rolling count of number of messages- even if the metric falls down to zero eventually. One possibility would be to publish a steady number of dummy messages which would advance the watermark and fire the panes but are filtered out later within the pipeline. The problem with this approach is that the publishing source needs to be adapted and that might not always be convenient/possible. 

Another one, imemented herein, would involve generating this fake data as another input and co-group it with the main stream. The advantage is that everything can be done in Dataflow without the need to tweak the source or the sink.

The inputs are divided in two streams. For the dummy one, I used `GenerateSequence` to create a new element every 5 seconds. I then window the PCollection (windowing strategy needs to be compatible with the one for the main stream so I will use the same for both). Then I map the element to a key-value pair where the value is 0 (we could use other values as we know from which stream the element comes but I want to evince that dummy records are not counted).

```java
PCollection<KV<String,Integer>> dummyStream = p
	.apply("Generate Sequence", GenerateSequence.from(0).withRate(1, Duration.standardSeconds(5)))
	.apply("Window Messages - Dummy", Window.<Long>into(
			...
	.apply("Count Messages - Dummy", ParDo.of(new DoFn<Long, KV<String, Integer>>() {
		@ProcessElement
		public void processElement(ProcessContext c) throws Exception {
			c.output(KV.of("num_messages", 0));
		}
	}));
```

For the main stream, that reads from Pub/Sub, I map each record to value 1. Later on, I will add all the ones as in typical word count examples using map-reduce stages.

```java
PCollection<KV<String,Integer>> mainStream = p
	.apply("Get Messages - Data", PubsubIO.readStrings().fromTopic(topic))
	.apply("Window Messages - Data", Window.<String>into(
			...
	.apply("Count Messages - Data", ParDo.of(new DoFn<String, KV<String, Integer>>() {
		@ProcessElement
		public void processElement(ProcessContext c) throws Exception {
			c.output(KV.of("num_messages", 1));
		}
	}));
```

Then we need to join them using a `CoGroupByKey` (I used the same `num_messages` key to group counts). This stage will output results when one of the two inputs has elements, therefore unblocking the main issue here (empty windows with no Pub/Sub messages).

```java
    final TupleTag<Integer> dummyTag = new TupleTag<>();
    final TupleTag<Integer> dataTag = new TupleTag<>();
    
    PCollection<KV<String, CoGbkResult>> coGbkResultCollection = KeyedPCollectionTuple.of(dummyTag, dummyStream)
    		.and(dataTag, mainStream).apply(CoGroupByKey.<String>create());
```

Finally, we add all the ones to obtain the total number of messages for the window. If there are no elements coming from `dataTag` then the sum will just default to 0.

```java
    public void processElement(ProcessContext c, BoundedWindow window) {
    	Integer total_sum = new Integer(0);
    
    	Iterable<Integer> dataTagVal = c.element().getValue().getAll(dataTag);
    	for (Integer val : dataTagVal) {
    		total_sum += val;
    	}
    
    	LOG.info("Window: " + window.toString() + ", Number of messages: " + total_sum.toString());
    }
```

This should result in something like: 
![Screenshot from 2019-02-05 21-52-09](https://user-images.githubusercontent.com/29493411/54085666-bd099300-4340-11e9-87dc-eaba15b3d65c.png)

When there is no data we will still see "Number of messages: 0" as desired. Note that results from different windows can come slightly unordered.


## License

These examples are provided under the Apache License 2.0.

## Issues

Report any issue to the GitHub issue tracker.
