# Logging GBK

WIP

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

WIP

![screenshot from 2018-09-03 21-17-41](https://user-images.githubusercontent.com/29493411/44999634-1b833c80-afbf-11e8-8fe4-e22f132f6d95.png)

## Next Steps

* Finish readme
* Use a side output to divert logging while grouped elements continue down the pipeline
* Define option to toggle on and off Stackdriver Logging and BigQuery Writes
* If possible, use generic types

## License

These examples are provided under the Apache License 2.0.

## Issues

Report any issue to the GitHub issue tracker.
