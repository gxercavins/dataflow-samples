# Adaptive triggers
## Different behavior at the start and end of the window

In this example we'll 'hack' the trigger behavior to write results more often at the start of the window than later on. A possible use case would be to implement this in mobile games and gather more info about short sessions, when the user is more active, and relax the data points we read for long ongoing sessions.

Disclaimer: example modified from base code [here](https://github.com/GoogleCloudPlatform/training-data-analyst/blob/master/courses/data_analysis/lab2/javahelp/src/main/java/com/google/cloud/training/dataanalyst/javahelp/StreamDemoConsumer.java) - `@author vlakshmanan`, be sure to check his [book](http://shop.oreilly.com/product/0636920057628.do) and [courses](https://www.coursera.org/specializations/gcp-data-machine-learning).

## Quickstart

You can use the provided `run.sh` script (don't forget to add execution permissions `chmod +x run.sh`) as in:
```
./run.sh <DATAFLOW_PROJECT_ID> <BUCKET_NAME> <PUB/SUB_INPUT_TOPIC> <BIGQUERY_PROJECT_ID:DATASET.TABLE>
```

Alternatively, follow these steps:
* Set up [authentication](https://cloud.google.com/docs/authentication/) your preferred way 
* Set the `$PROJECT`, `$BUCKET`, `$TOPIC` and `$TABLE` variables and run the Dataflow job:
```
mvn compile -e exec:java \
 -Dexec.mainClass=com.dataflow.samples.AdaptiveTriggers \
      -Dexec.args="--project=$PROJECT \
      --stagingLocation=gs://$BUCKET/staging/ \
      --tempLocation=gs://$BUCKET/staging/ \
      --input=projects/$PROJECT/topics/$TOPIC \
      --output=$TABLE \
      --runner=DataflowRunner"
```
* If you need dummy data you can use the `publish.py` example, adapted from the [quickstart](https://cloud.google.com/pubsub/docs/quickstart-client-libraries#pubsub-client-libraries-python)
* Install the `google-cloud-pubsub` Python package: `pip install google-cloud-pubsub` (use of `virtualenv` is recommended)
* Run the `publish.py` script to start publishing messages: `python publish.py $PROJECT publish $TOPIC`

This code was tested with Java SDK 2.5.0.

## Example

The main idea is to use `c.pane().getIndex()` in `processElement` inside the ParDo. We assign this value to a variable named `index` and we'll call `c.output()` for the first six panes (`index < 6`) and, past that, only for 1-out-of-6 panes (`index % 6 == 0`). We'll also record the Pane timing (`EARLY`, `ON_TIME` or `LATE`):
```
.apply("ToBQRow", ParDo.of(new DoFn<Integer, TableRow>() {
				@ProcessElement
				public void processElement(ProcessContext c) throws Exception {
					Long index = c.pane().getIndex();
					if (index < 6 || index % 6 == 0) {
						TableRow row = new TableRow();
						row.set("timestamp", Instant.now().toString());
						row.set("num_words", c.element());
						row.set("pane_index", index);
						row.set("pane_timing", c.pane().getTiming().toString());
						c.output(row);
					}
				}
}))
```

Note that this can be done as we are using `.accumulatingFiredPanes()` in our windowing strategy and, as we are not discarding them, we are not losing data when dropping the unnecessary panes.

The result can be seen below:

![screenshot from 2018-08-01 12-31-58](https://user-images.githubusercontent.com/29493411/43517259-54b2b91e-9588-11e8-8ba6-667a905ed156.png)

Note that the publish script is adding 3-word messages (`data = u'Message number {}'.format(n)`) every 5 seconds to the topic (`time.sleep(5)`). As we are firing `.withEarlyFirings(AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.standardSeconds(5))` we are getting an `EARLY` pane every 10 seconds with 6 words (3x2), as can be observed in the previous image for pane indexes 0-6. From there on, only the panes that are multiple numbers of 6 get written to the output table (12, 18, 24 and so on) which show, more or less, increments of 36 words (6x6) so no data is dropped in-between firings.

After the 10min window is closed (`FixedWindows.of(Duration.standardMinutes(10))`), the pane index is reset back to 0 and the more frequent output of counts is resumed once again.

## Shutdown

The publish script will go on for approximately 50min. Be sure to interrupt it (`CTRL+C`) or adjust the number of messages to something lower than 600, if needed.

As a streaming job this will run until we issue the drain/cancel command. You can do so using the UI, the API, Client libraries or the `gcloud` command, for example (you might need to account for the `region` if specified at runtime):
```
gcloud dataflow jobs cancel $(gcloud dataflow jobs list | grep -m 1 adaptivetriggers | cut -f 1 -d " ")
```

## License

These examples are provided under the Apache License 2.0.

## Issues

Report any issue to the GitHub issue tracker.
