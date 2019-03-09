# Empty windows
## ToDo: summary

https://stackoverflow.com/questions/54503190/live-monitoring-using-apache-beam/54543527#54543527

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

ToDo: Explain code

## License

These examples are provided under the Apache License 2.0.

## Issues

Report any issue to the GitHub issue tracker.
