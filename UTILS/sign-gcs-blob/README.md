# Sign GCS Blob inside a ParDo

In order to sign a file using the GCS client we need a private key instead of just using an authorization token. How can we read from a JSON credentials file stored in GCS? Here we'll present a Python example written as an answer to a [StackOverflow question](https://stackoverflow.com/questions/59557617/how-to-sign-gcs-blob-from-the-the-dataflow-worker/59600942).

## Example
We can see an example [here](https://github.com/apache/beam/blob/v2.16.0/sdks/python/apache_beam/examples/wordcount.py#L75) on how to add custom options to our Beam pipelines. With this we can create a `--key_file` argument that will point to the credentials stored in GCS:

```python
parser.add_argument('--key_file',
                  dest='key_file',
                  required=True,
                  help='Path to service account credentials JSON.')
```

This will allow us to add the `--key_file gs://PATH/TO/CREDENTIALS.json` flag when running the job. 

Then, we can read it from within the job and pass it as a side input to the `DoFn` that needs to sign the blob. Starting from the example [here](https://stackoverflow.com/a/59504952/6121516) we create a `credentials` PCollection to hold the JSON file:

```python
credentials = (p 
  | 'Read Credentials from GCS' >> ReadFromText(known_args.key_file))
```

and we broadcast it to all workers processing the `SignFileFn` function:

```python
(p
  | 'Read File from GCS' >> beam.Create([known_args.input]) \
  | 'Sign File' >> beam.ParDo(SignFileFn(), pvalue.AsList(credentials)))
```

Inside the `ParDo`, we build the JSON object to initialize the client (using the approach suggested [here](https://github.com/googleapis/google-cloud-python/issues/7291#issuecomment-471982254)) and sign the file:

```python
class SignFileFn(beam.DoFn):
  """Signs GCS file with GCS-stored credentials"""
  def process(self, gcs_blob_path, creds):
    from google.cloud import storage
    from google.oauth2 import service_account

    credentials_json=json.loads('\n'.join(creds))
    credentials = service_account.Credentials.from_service_account_info(credentials_json)

    gcs_client = storage.Client(credentials=credentials)

    bucket = gcs_client.get_bucket(gcs_blob_path.split('/')[2])
    blob = bucket.blob('/'.join(gcs_blob_path.split('/')[3:]))

    url = blob.generate_signed_url(datetime.timedelta(seconds=300), method='GET')
    logging.info(url)
    yield url
```

See full code in `credentials-in-side-input.py`.


## License

This example is provided under the Apache License 2.0.

## Issues

Report any issue to the GitHub issue tracker.
