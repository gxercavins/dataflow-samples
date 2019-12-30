# Schema in GCS file

In this example, originally written as an [answer](https://stackoverflow.com/a/59504952/6121516) to a StackOverflow question, we want to use a schema stored in GCS to incorporate it into our data. We'll explore two ways to do so: using side inputs and the start bundle method.

## Example

To start, we'll download a BigQuery schema from a public table (`schema.json`) and upload it to GCS:

```bash
bq show --schema bigquery-public-data:usa_names.usa_1910_current > schema.json
gsutil cp schema.json gs://$BUCKET
```

We can pretty print it using `json.tool`:

```bash
$ python3 -m json.tool < schema.json
[
    {
        "description": "2-digit state code",
        "type": "STRING",
        "name": "state",
        "mode": "NULLABLE"
    },
    {
        "description": "Sex (M=male or F=female)",
        "type": "STRING",
        "name": "gender",
        "mode": "NULLABLE"
    },
    {
        "description": "4-digit year of birth",
        "type": "INTEGER",
        "name": "year",
        "mode": "NULLABLE"
    },
    {
        "description": "Given name of a person at birth",
        "type": "STRING",
        "name": "name",
        "mode": "NULLABLE"
    },
    {
        "description": "Number of occurrences of the name",
        "type": "INTEGER",
        "name": "number",
        "mode": "NULLABLE"
    }
]
```

Our input data will be some schema-less rows in csv format so that we have to use the auxiliary GCS schema:

```python
data = [('NC', 'F', 2020, 'Hello', 3200),
        ('NC', 'F', 2020, 'World', 3180)]
```

---

## Using side inputs

We read the JSON file into a `schema` PCollection:

```python
schema = (p 
  | 'Read Schema from GCS' >> ReadFromText('gs://{}/schema.json'.format(BUCKET)))
```

and then we pass it to the `ParDo` as a side input so that it's broadcasted to every worker that executes the `DoFn`. In this case, we can use `AsSingleton` as we just want to supply the schema as a single value:

```python
(p
  | 'Create Events' >> beam.Create(data) \
  | 'Enrich with side input' >> beam.ParDo(EnrichElementsFn(), pvalue.AsSingleton(schema)) \
  | 'Log elements' >> beam.ParDo(LogElementsFn()))
```

Now we can access the `schema` in the `process` method of `EnrichElementsFn`:

```python
class EnrichElementsFn(beam.DoFn):
  """Zips data with schema stored in GCS"""
  def process(self, element, schema):
    field_names = [x['name'] for x in json.loads(schema)]
    yield zip(field_names, element)
```

Code in `schema-in-side-input.py`.

---

## Using start bundle

In this case we don't pass any additional input to the `ParDo`:

```python
(p
  | 'Create Events' >> beam.Create(data) \
  | 'Enrich with start bundle' >> beam.ParDo(EnrichElementsFn()) \
  | 'Log elements' >> beam.ParDo(LogElementsFn()))
```

And now we use the Python Client Library (we need to install `google-cloud-storage`) to read the schema each time that a worker initializes a bundle:

```python
class EnrichElementsFn(beam.DoFn):
  """Zips data with schema stored in GCS"""
  def start_bundle(self):
    from google.cloud import storage

    client = storage.Client()
    blob = client.get_bucket(BUCKET).get_blob('schema.json')
    self.schema = blob.download_as_string()

  def process(self, element):
    field_names = [x['name'] for x in json.loads(self.schema)]
    yield zip(field_names, element)
```

Code in `schema-in-start-bundle.py`.

---

The output is the same in both cases:

```python
INFO:root:[(u'state', 'NC'), (u'gender', 'F'), (u'year', 2020), (u'name', 'Hello'), (u'number', 3200)]
INFO:root:[(u'state', 'NC'), (u'gender', 'F'), (u'year', 2020), (u'name', 'World'), (u'number', 3180)]
```

Tested with 2.16.0 SDK and the `DirectRunner`.


## License

This example is provided under the Apache License 2.0.

## Issues

Report any issue to the GitHub issue tracker.
