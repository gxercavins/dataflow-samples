# Retrieve Job ID

This example is intended to be staged as a template and be able to retrieve, from within the pipeline, which is the actual job ID each time we execute it. Originally written as an answer to a StackOverflow [question](https://stackoverflow.com/questions/52374924/google-cloud-dataflow-python-retrieving-job-id/).

## Quickstart

* Set up [authentication](https://cloud.google.com/docs/authentication/) your preferred way
* Install the `apache-beam[gcp]` Python package: `apache-beam[gcp]` (use of `virtualenv` is recommended)
* Replace the parameters in the `retrieve_job_id` function specifying the correct Project ID, job name and region
* Set the `$PROJECT`, `$BUCKET` and stage the Dataflow template:
```bash
python job_id.py \
    --runner DataflowRunner \
    --project $PROJECT \
    --staging_location gs://$BUCKET/staging \
    --temp_location gs://$BUCKET/temp \
    --template_location gs://$BUCKET/templates/retrieve_job_id
```

* Specify the desired job name (`myjobprefix` in my case, has to match the prefix defined in the code) when executing the templated job:
```bash
gcloud dataflow jobs run myjobprefix \
   --gcs-location gs://$BUCKET/templates/retrieve_job_id
```

This code was tested with `apache-beam[gcp]==2.6.0`.

## Example

The list of recent Dataflow jobs can be retrieved calling the API endpoint [`dataflow.projects().locations().jobs().list`](https://cloud.google.com/dataflow/docs/reference/rest/v1b3/projects.locations.jobs/list) from within the pipeline. In this case we asume that we always invoke the template with the same job name, otherwise the template should be modified so that the job prefix is passed as a runtime parameter with [`ValueProvider`](https://cloud.google.com/dataflow/docs/templates/creating-templates#using-valueprovider-in-your-pipeline-options).

```python
result = dataflow.projects().locations().jobs().list(
  projectId=project,
  location=location,
).execute()
```

The list of jobs is parsed applying a regex to see if the job contains the job name prefix (`job_prefix`) and, if so, returns the job ID. In case that more than one job with the same name exists we are only interested in the current job run. Therefore, we'll stop reading and return after the first occurrence as more recent jobs are listed first, hence the `break` statement:

```python
for job in result['jobs']:
  if re.findall(r'' + re.escape(job_prefix) + '', job['name']):
    job_id = job['id']
    break
```

## License

These examples are provided under the Apache License 2.0.

## Issues

Report any issue to the GitHub issue tracker.
