# Which jobs run with a certain SDK

Quick snippet to showcase how to retrieve the list of Job IDs that use(d) a certain version of the SDK

## Quickstart

Set up authentication and install the following dependencies:
```bash
pip install --upgrade google-api-python-client oauth2client
```

Modify the parameters in the script:
```python
# change the parameters accordingly
project = 'PROJECT_ID'
location = 'us-central1'
sdk = '2.5.0'
```

By default the code will look for jobs in `us-central1` using the `2.5.0` SDK. Replace the value of the `sdk` variable for the desired version.

Then run the script:
```bash
python sdk.py
```

Tested with:
* `google-api-python-client==1.7.4`
* `oauth2client==4.1.3`

## Example

First of all we'll retrieve a list of all recent jobs by calling the corresponding regional [endpoint](https://cloud.google.com/dataflow/docs/reference/rest/v1b3/projects.locations.jobs/list):

```python
# list all recent jobs 
dataflow = build('dataflow', 'v1b3', credentials=credentials)
results = dataflow.projects().locations().jobs().list(projectId=project, location=location).execute()
```

We can use the `json` library to pretty-print a couple of job results:

```python
print json.dumps(job, sort_keys=False, indent=4)
```

```json
{
    "name": "test-template", 
    "projectId": "PROJECT_ID", 
    "createTime": "2018-10-16T11:03:52.960890Z", 
    "jobMetadata": {
        "sdkVersion": {
            "versionDisplayName": "Apache Beam SDK for Java", 
            "version": "2.6.0", 
            "sdkSupportStatus": "SUPPORTED"
        }
    }, 
    "currentStateTime": "2018-10-16T11:09:55.186765Z", 
    "type": "JOB_TYPE_STREAMING", 
    "id": "2018-10-16_04_03_51-NUMERIC_SUFFIX", 
    "currentState": "JOB_STATE_CANCELLED", 
    "location": "us-central1"
}
```
```json
{
    "name": "fusedpipeline-guillem-0930181210-c01457ee", 
    "projectId": "PROJECT_ID", 
    "createTime": "2018-09-30T18:12:18.777525Z", 
    "currentStateTime": "2018-09-30T18:14:45.179556Z", 
    "type": "JOB_TYPE_STREAMING", 
    "id": "2018-09-30_11_12_17-NUMERIC_SUFFIX", 
    "currentState": "JOB_STATE_CANCELLED", 
    "location": "us-central1"
}
```

In the first result we can retrieve the SDK with `job['jobMetadata']['sdkVersion']['version']`. However, these metadata fields are not included in the second case. Therefore, we'll need to describe each of the previous jobs with [`jobs().get()`](https://cloud.google.com/dataflow/docs/reference/rest/v1b3/projects.locations.jobs/get):

```python
# check SDK version for each job
for job in results['jobs']:
  job_description = dataflow.projects().locations().jobs().get(projectId=project, location=location, jobId=job['id']).execute()
  job_sdk = job_description['jobMetadata']['sdkVersion']['version']
  if job_sdk  == sdk:
    job_list.append(job['id'])
```

Now the API response includes a complete description of the job including current stage state. We can retrieve the SDK version with either `job_description['environment']['userAgent']['version']` or `job_description['jobMetadata']['sdkVersion']['version']`:

```json
{
    "name": "test-template", 
    "projectId": "PROJECT_ID", 
    "labels": {
        "goog-dataflow-provided-template-version": "2018-10-08-00_rc00", 
        "goog-dataflow-provided-template-name": "cloud_pubsub_to_gcs_text"
    }, 
    "createTime": "2018-10-16T11:03:52.960890Z", 
    "environment": {
        "userAgent": {
            "version": "2.6.0", 
            "name": "Apache Beam SDK for Java", 
            "os.version": "4.3.5-smp-815.12.0.0", 
            "os.arch": "amd64", 
            "java.version": "1.8.0_151-google-v7", 
            "legacy.environment.major.version": "7", 
            "container.version": "beam-2.6.0", 
            "fnapi.environment.major.version": "7", 
            "os.name": "Linux", 
            "java.vendor": "Google Inc."
        }, 
        "version": {
            "major": "7", 
            "job_type": "STREAMING"
        }
    }, 
    "jobMetadata": {
        "sdkVersion": {
            "versionDisplayName": "Apache Beam SDK for Java", 
            "version": "2.6.0", 
            "sdkSupportStatus": "SUPPORTED"
        }
    }, 
    "currentStateTime": "2018-10-16T11:09:55.186765Z", 
    "stageStates": [
        {
            "executionStageName": "F20", 
            "executionStageState": "JOB_STATE_CANCELLED", 
            "currentStateTime": "2018-10-16T11:07:29.026Z"
        }, 
        ...
    ], 
    "type": "JOB_TYPE_STREAMING", 
    "id": "2018-10-16_04_03_51-NUMERIC_SUFFIX", 
    "currentState": "JOB_STATE_CANCELLED", 
    "location": "us-central1"
}
```

Another possible use case would be to look for `sdkSupportStatus` fields different than `SUPPORTED` such as `STALE`.

## License

These examples are provided under the Apache License 2.0.

## Issues

Report any issue to the GitHub issue tracker.
