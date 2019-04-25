# Stop Dataflow job

Quick snippet to showcase how to stop a Dataflow job programmatically

## Quickstart

Install the following dependencies:
``` bash
pip install --upgrade google-api-python-client oauth2client
```

Modify the parameters in the script:
``` python
# change the parameters accordingly
project='PROJECT_ID'
job='JOB_ID'
location='REGION'
```

and run the script:
``` bash
python stop.py
```

Tested with:
* `google-api-python-client==1.7.4`
* `oauth2client==4.1.2`

## Example

Dataflow jobs can be stopped through the UI or using the `gcloud` [command](https://cloud.google.com/sdk/gcloud/reference/dataflow/jobs/cancel). However, if we inspect the Dataflow API, we can see that the jobs REST [collections](https://cloud.google.com/dataflow/docs/reference/rest/) do not have a `cancel` method.

If we trace the aforementioned command by running:
``` bash
gcloud dataflow jobs cancel <JOB_ID> --log-http
```

We'll see the following output snippets:
``` bash
==== request start ====
uri: https://dataflow.googleapis.com/v1b3/projects/<PROJECT_ID>/locations/<REGION>/jobs/<JOB_ID>?alt=json
method: PUT
...
== body start ==
{"requestedState": "JOB_STATE_CANCELLED"}
== body end ==
```

Basically, we update the job with a request to [`projects.locations.jobs.update`](https://cloud.google.com/dataflow/docs/reference/rest/v1b3/projects.locations.jobs/update) specifying that the desired state is `JOB_STATE_CANCELLED`.

Translating this into Python code:
``` python
result = dataflow.projects().locations().jobs().update(
	projectId=project,
	jobId=job,
	location=location,
	body=state
).execute()
```
where `state` is `{"requestedState": "JOB_STATE_CANCELLED"}`.

## License

These examples are provided under the Apache License 2.0.

## Issues

Report any issue to the GitHub issue tracker.
