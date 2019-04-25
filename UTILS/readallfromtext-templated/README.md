## ReadAllFromText and runtime parameters

[`ReadAllFromText`](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/io/textio.py#L423) expects to read from a PCollection of files instead of passing it as an argument. One possible way to use it is with the `Create` function:
```python
p | beam.Create(["input.csv"])
  | beam.io.ReadAllFromText()
```

However, it accepts only in-memory data and not runtime parameters as it needs to know the pipeline graph structure before-hand. We want to be able to stage it as a template and change the input files at runtime.

## Quickstart

* Set up [authentication](https://cloud.google.com/docs/authentication/) your preferred way
* Install the `apache-beam[gcp]` Python package: `sudo pip install apache-beam[gcp]` (use of `virtualenv` is recommended)
* Replace the output bucket in code: `WriteToText("gs://BUCKET-NAME/path/to/output.txt")`
* Set the `$PROJECT`, `$BUCKET` and stage the Dataflow template:
```bash
python readall.py \
    --runner DataflowRunner \
    --project $PROJECT \
    --staging_location gs://$BUCKET/staging \
    --temp_location gs://$BUCKET/temp \
    --template_location gs://$BUCKET/templates/readall
```

* Specify the input GCS path parameter (`$INPUT` in my case) when executing the templated job:
```bash
gcloud dataflow jobs run readall \
   --gcs-location gs://$BUCKET/templates/readall \
   --input $INPUT
```

This code was tested with `apache-beam[gcp]==2.10.0`.

## Example

One possible way to do it would be to read the PCollection of files we need from another source (i.e. always write the list of files to a particular GCS object). A more dynamic workaround is presented here. We create a PCollection with a single string (any, it doesn't really matter) to kickstart the pipeline. Then we use a `Map` function that will read it but don't do anything with it. Instead, it will just output the runtime parameter so that it can be passed to `ReadAllFromText` as we want:

```python
p
  | 'Create' >> beam.Create(['Start!']) # just to kickstart the pipeline
  | 'Read Input Parameter' >> beam.Map(lambda x: custom_options.input.get()) # Map will accept the template parameter
  | 'Read All Files' >> beam.io.ReadAllFromText()
```

## License

These examples are provided under the Apache License 2.0.

## Issues

Report any issue to the GitHub issue tracker.
