# Write to different BigQuery tables with DynamicDestinations

The `DynamicDestinations` class allows you to write to different BigQuery tables dynamically as a function of an input element. In this case each record might contain a different number of features (different schema) with the first column indicating the number.

## Quickstart

You can use the provided `run.sh` script (don't forget to add execution permissions `chmod +x run.sh`) as in:
```
./run.sh <DATAFLOW_PROJECT_ID> <BUCKET_NAME(no gs://)> <BIGQUERY_PROJECT_ID>:<DATASET>.<TABLE_PREFIX>
```

Alternatively, follow these steps:
* Install the `lorem`package: `pip install lorem` (use of `virtualenv` is recommended)
* Run the `generate.py` script to create an `input.csv` file that will be used as input data. Defaults to 100 records split with up  to 10 possible different schemas.
* Set up [authentication](https://cloud.google.com/docs/authentication/) your preferred way 
* Set the `$BUCKET` variable (`export BUCKET=BUCKET_NAME`) and upload the file: `gsutil cp input.csv gs://$BUCKET`
* Set the `$PROJECT` and `$TABLE` variables too and run the Dataflow job:
```
mvn compile -e exec:java \
 -Dexec.mainClass=com.dataflow.samples.WriteToDifferentTables \
      -Dexec.args="--project=$PROJECT \
      --stagingLocation=gs://$BUCKET/staging/ \
      --tempLocation=gs://$BUCKET/staging/ \
      --input=gs://$BUCKET/input.csv \
      --output=$TABLE \
      --runner=DataflowRunner"
```

Note: for very large files and output number of tables (i.e. 2000) you might need to use larger [machine types](https://cloud.google.com/compute/docs/machine-types) and be aware of [BigQuery quotas](https://cloud.google.com/bigquery/quotas).

This was tested with Java SDK 2.5.0.

## Example

Input data will have varying number of String fields with a first column indicating the number. Quick file preview:

```
$ head input.csv 
"3,Dolor,tempora,ipsum"
"1,Voluptatem"
"2,Amet,magnam"
"8,Magnam,dolorem,est,quiquia,voluptatem,numquam,voluptatem,modi"
"10,Dolorem,dolorem,sit,modi,amet,neque,porro,dolore,quisquam,etincidunt"
```

As they have different schemas we want to route them to different tables. In this example, we'll use the `dynamic` prefix and we'll write the first record to `dynamic3` as it has 3 fields, second one to `dynamic1` and so on.

For determining `getDestination` we'll use the first field: `element.getValue().split(",")[0]`, which will return `3, 1, 2, 8, 10`, etc. And we'll prepend the table prefix in `getTable`. In `getSchema` we'll generate a different one for each output according to number of columns (a better implementation would be to generate them beforehand).

In `.withFormatFunction` we'll write each input field to the corresponding `TableRow` field.

The result can be seen below:

![screenshot from 2018-07-31 22-07-28](https://user-images.githubusercontent.com/29493411/43510931-6ae0db3c-9577-11e8-9ec5-91cf51635487.png)

## License

These examples are provided under the Apache License 2.0.

## Issues

Report any issue to the GitHub issue tracker.
