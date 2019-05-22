# One Window One File

In this example we want to write elements belonging to the same window to a single output file. This was originally an answer to a StackOverflow [question](https://stackoverflow.com/questions/56234318/write-to-one-file-per-window-in-dataflow-using-python/).

## Quickstart

* Set up [authentication](https://cloud.google.com/docs/authentication/) your preferred way
* Install the `apache-beam[gcp]` Python package: `apache-beam[gcp]` (use of `virtualenv` is recommended)
* Run the code locally:
```bash
python one-window-one-file.py
```

This code was tested with `apache-beam[gcp]==2.12.0`.

## Example

The approach that we'll follow is to group elements into key-value pairs where the key is the window that they belong to. With `filesystems.FileSystems.create` we can control how do we want to write the files.

For this example we will be using 10s windows and some dummy data where events are separated 4s each. In order to have some reproducible tests without the need of having Pub/Sub messages being published, we'll generate our input data from this list:

```python
data = [{'event': '{}'.format(event), 'timestamp': time.time() + 4*event} for event in range(10)]
```

The `timestamp` field will add 4s increments to each successive message. This way, we can assign element timestamp (again, this is just to emulate Pub/Sub events in a controlled way). After that, we window the events, use the windowing info as the key, group by key and write the results to the `output` folder:

```python
events = (p
  | 'Create Events' >> beam.Create(data) \
  | 'Add Timestamps' >> beam.Map(lambda x: beam.window.TimestampedValue(x, x['timestamp'])) \
  | 'Add Windows' >> beam.WindowInto(window.FixedWindows(10)) \
  | 'Add Window Info' >> beam.ParDo(AddWindowingInfoFn()) \
  | 'Group By Window' >> beam.GroupByKey() \
  | 'Windowed Writes' >> beam.ParDo(WindowedWritesFn('output/')))
```

Where `AddWindowingInfoFn` is pretty straightforward. It simply accesses windowing information and adds it as the key of the processed element:

```python
class AddWindowingInfoFn(beam.DoFn):
  """output tuple of window(key) + element(value)"""
  def process(self, element, window=beam.DoFn.WindowParam):
    yield (window, element)
```

With `WindowedWritesFn` we write to the base path that we specified in the pipeline (`output/` folder in my case, used in `__init__`). Then, we construct the filename using the window. For convenience, we can convert the epoch timestamps to human-readable dates. Finally, we iterate over all the elements and write them to the corresponding file (into different lines). Of course, this behavior can be tuned at will in this function:

```python
class WindowedWritesFn(beam.DoFn):
    """write one file per window/key"""
    def __init__(self, outdir):
        self.outdir = outdir
        
    def process(self, element):
        (window, elements) = element
        window_start = str(window.start.to_utc_datetime()).replace(" ", "_")
        window_end = str(window.end.to_utc_datetime()).replace(" ", "_")
        writer = filesystems.FileSystems.create(self.outdir + window_start + ',' + window_end + '.txt')

        for row in elements:
          writer.write(str(row)+ "\n")

        writer.close()
```

This will write elements belonging to each window to a different file. In my case there are 5 different windows and, thus, output files:

```bash
$ ls output/
2019-05-21_19:01:20,2019-05-21_19:01:30.txt
2019-05-21_19:01:30,2019-05-21_19:01:40.txt
2019-05-21_19:01:40,2019-05-21_19:01:50.txt
2019-05-21_19:01:50,2019-05-21_19:02:00.txt
2019-05-21_19:02:00,2019-05-21_19:02:10.txt
```

The first one only contains element 0 (but this will vary between executions):

```bash
$ cat output/2019-05-21_19\:01\:20\,2019-05-21_19\:01\:30.txt 
{'timestamp': 1558465286.933727, 'event': '0'}
```

The second one contains elements 1 to 3 and so on:

```bash
$ cat output/2019-05-21_19\:01\:30\,2019-05-21_19\:01\:40.txt 
{'timestamp': 1558465290.933728, 'event': '1'}
{'timestamp': 1558465294.933728, 'event': '2'}
{'timestamp': 1558465298.933729, 'event': '3'}
```

Caveat from this approach is that all elements from the same window are grouped into the same worker. This would happen anyway if writing to a single shard or output file as per the imposed use case but, for higher loads, we might need to consider moving to larger machine types.

## License

These examples are provided under the Apache License 2.0.

## Issues

Report any issue to the GitHub issue tracker.
