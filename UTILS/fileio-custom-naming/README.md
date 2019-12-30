# FileIO Custom Naming Function

In this example, originally written as an [answer](https://stackoverflow.com/a/59459063/6121516) to a StackOverflow question, we want to modify the default [naming pattern](https://beam.apache.org/releases/pydoc/2.16.0/apache_beam.io.fileio.html?highlight=default_file_naming) when using `fileio.WriteToFiles()`. In particular, we want to write each JSON to a different file and incorporate the hash of the record into the file name (i.e. `<HASH>.json`).

## Example

We will base our example on [this test](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/io/fileio_test.py). To match our use case, we'll specify a different `destination` for each record according to their hash as we want to write each element to a different file. In addition, we'll pass our custom naming function called `hash_naming`:

```python
data = [{'id': 0, 'message': 'hello'},
        {'id': 1, 'message': 'world'}]

(p
  | 'Create Events' >> beam.Create(data) \
  | 'JSONify' >> beam.Map(json.dumps) \
  | 'Print Hashes' >> beam.ParDo(PrintHashFn()) \
  | 'Write Files' >> fileio.WriteToFiles(
      path='./output',
      destination=lambda record: hash(record),
      sink=lambda dest: JsonSink(),
      file_naming=hash_naming))
```

In `PrintHashFn` we'll log each element with the corresponding hash:

```python
logging.info("Element: %s with hash %s", element, hash(element))
```

so that, for our data, we'll get:

```python
INFO:root:Element: {"message": "hello", "id": 0} with hash -1885604661473532601
INFO:root:Element: {"message": "world", "id": 1} with hash 9144125507731048840
```

There might be a better way but I found out that, by invoking `fileio.destination_prefix_naming()(*args)`, we can retrieve the destination (`-1885604661473532601`) from the default naming scheme (`-1885604661473532601----00000-00001`):

```python
def hash_naming(*args):
  file_name = fileio.destination_prefix_naming()(*args)  # -1885604661473532601----00000-00001
  destination = file_name.split('----')[0]  # -1885604661473532601
  return '{}.json'.format(destination)  # -1885604661473532601.json
```

Note that the split to get the substring might be different if we add windowing into the mix.

Running the script with 2.16.0 SDK and the `DirectRunner` we can get the following output:

```bash
$ ls output/
-1885604661473532601.json  9144125507731048840.json
$ cat output/-1885604661473532601.json 
"{\"message\": \"hello\", \"id\": 0}"
```

Full code in `fileio-naming.py` file.

## License

This example is provided under the Apache License 2.0.

## Issues

Report any issue to the GitHub issue tracker.
