# Global Options

This example shows how to define custom options in a template, pass them at runtime and make them accessible globally so we can use them inside other functions such as a ParDo.

## Quickstart

You can use the provided `run.sh` script (don't forget to add execution permissions `chmod +x run.sh`) as in:
```bash
./run.sh <DATAFLOW_PROJECT_ID> <BUCKET_NAME>
```

Alternatively, follow these steps:
* Set up [authentication](https://cloud.google.com/docs/authentication/) your preferred way
* Install the `apache-beam[gcp]` Python package: `apache-beam[gcp]` (use of `virtualenv` is recommended)
* Set the `$PROJECT`, `$BUCKET` and stage the Dataflow template:
```bash
python global.py \
    --runner DataflowRunner \
    --project $PROJECT \
    --staging_location gs://$BUCKET/staging \
    --temp_location gs://$BUCKET/temp \
    --template_location gs://$BUCKET/templates/global_options
```
* Run the Dataflow job by invoking the template (feel free to change the runtime parameters):
```bash
gcloud dataflow jobs run global_options \
   --gcs-location gs://$BUCKET/templates/global_options \
   --parameters path=test_path,table_name=test_table
```

This code was tested with `apache-beam[gcp]==2.5.0`.

## Example

This was originally an answer to a StackOverflow [question](https://stackoverflow.com/questions/52188858/python-dataflow-template-making-runtime-parameters-globally-accessible/). Base code was provided by user `jmoore255` and this example only intends to explain how to ensure those options are globally available.

The `CustomPipelineOptions` class extends the `PipelineOptions` one to add two [`RuntimeValueProvider`](https://cloud.google.com/dataflow/docs/templates/creating-templates#using-valueprovider-in-your-functions) parameters: `path` and `test_table`. When we run the template we provide those options at runtime as in:
```bash
--parameters path=test_path,table_name=test_table
```

We try to access those values using the getter method inside the `rewrite_values` function:
```python
file_path = custom_options.path.get()
table_name = custom_options.table_name.get()
```

Unfortunately, we'll get the following error:
> RuntimeError: KeyError: NameError("global name 'custom_options' is not defined",) [while running 'FlatMap(rewrite_values)']

The simplest solution is to declare those as global variables:
```python
global cloud_options
global custom_options

pipeline_options = PipelineOptions(pipeline_args)
cloud_options = pipeline_options.view_as(GoogleCloudOptions)
custom_options = pipeline_options.view_as(CustomPipelineOptions)
pipeline_options.view_as(SetupOptions).save_main_session = True
```

Now they can be accessed from the aforementioned step:

[image](https://i.stack.imgur.com/bze3Y.png)

## License

These examples are provided under the Apache License 2.0.

## Issues

Report any issue to the GitHub issue tracker.
