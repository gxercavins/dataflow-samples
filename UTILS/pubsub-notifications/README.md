## Pub/Sub notifications for GCS

This was originally written as an answer to a [StackOverflow question](https://stackoverflow.com/a/57497855/6121516).

[Pub/Sub notifications][1] will contain event metadata such as the recently uploaded object. Among other information we have the `id` field from which we can construct the full file path:

```python
{
	u'kind': u'storage#object', u'contentType': u'application/x-gzip', 
	...
	u'id': u'BUCKET_NAME/path/to/FILE_NAME/GENERATION_ID'
	...
}
```

In order to read the file contents, we will need to parse the notification to get the id and then pass the resulting PCollection to `beam.io.ReadAllFromText()` as in:

```python
class ExtractFn(beam.DoFn):
    def process(self, element):
    	file_name = 'gs://' + "/".join(element['id'].split("/")[:-1])
    	logging.info('File: ' + file_name) 
        yield file_name
```

Note that we prepended `gs://` to the `id` field and removed the generation field (version control).

The main pipeline is:

```python
(p
  | 'Read Messages' >> beam.io.ReadFromPubSub(topic="projects/PROJECT/topics/TOPIC")
  | 'Convert Message to JSON' >> beam.Map(lambda message: json.loads(message))
  | 'Extract File Names' >> beam.ParDo(ExtractFn())
  | 'Read Files' >> beam.io.ReadAllFromText()
  | 'Write Results' >> beam.ParDo(LogFn()))
```

Code tested with the direct runner and 2.14.0 SDK, the public file `gs://apache-beam-samples/shakespeare/kinglear.txt` and a test message (not a real notification): 

```bash
python notifications.py --streaming
gcloud pubsub topics publish $TOPIC_NAME --message='{"id": "apache-beam-samples/shakespeare/kinglear.txt/1565795872"}'
```

Running the script will start printing Shakespeare's King Lear script:

```bash
INFO:root:File: gs://apache-beam-samples/shakespeare/kinglear.txt
INFO:oauth2client.transport:Attempting refresh to obtain initial access_token
...
INFO:root:	KING LEAR
INFO:root:
INFO:root:
INFO:root:	DRAMATIS PERSONAE
INFO:root:
INFO:root:
INFO:root:LEAR	king of Britain  (KING LEAR:)
INFO:root:
INFO:root:KING OF FRANCE:
```


  [1]: https://cloud.google.com/storage/docs/pubsub-notifications

## License

These examples are provided under the Apache License 2.0.

## Issues

Report any issue to the GitHub issue tracker.
