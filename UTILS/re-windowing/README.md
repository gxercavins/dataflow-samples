# Re-windowing elements

ToDo: description. Originally written as an answer in [StackOverflow](https://stackoverflow.com/a/59200156/6121516).

## Quickstart

* Set up [authentication](https://cloud.google.com/docs/authentication/) your preferred way
* Install the `apache-beam[gcp]` Python package: `apache-beam[gcp]` (use of `virtualenv` is recommended)
* Execute `python re-window.py`

This code was tested locally with `apache-beam[gcp]==2.16.0`.

## Example

We can just apply a second windowing function, in this case `beam.WindowInto(window.GlobalWindows())`, to assign all elements to the global window. We'll have just a single in-memory message, we'll assign a timestamp with `beam.window.TimestampedValue()` and apply our first windowing function (`SlidingWindows()`). We'll print debugging information regarding the message and effective windowing before re-applying a second windowing (`GlobalWindows()`):

```python
data = [{'message': 'Hi', 'timestamp': time.time()}]

events = (p
  | 'Create Events' >> beam.Create(data) \
  | 'Add Timestamps' >> beam.Map(lambda x: beam.window.TimestampedValue(x, x['timestamp'])) \
  | 'Sliding Windows'   >> beam.WindowInto(beam.window.SlidingWindows(60, 60)) \
  | 'First window' >> beam.ParDo(DebugPrinterFn()) \
  | 'global Window'   >> beam.WindowInto(window.GlobalWindows()) \
  | 'Second window'   >> beam.ParDo(DebugPrinterFn()))
```

where `DebugPrinterFn` prints window information:

```python
class DebugPrinterFn(beam.DoFn):
  """Just prints the element and window"""
  def process(self, element, window=beam.DoFn.WindowParam):
    logging.info("Received message %s in window=%s", element['message'], window)
    yield element
```

And we get the following output:

```
INFO:root:Received message Hi in window=[1575565500.0, 1575565560.0)
INFO:root:Received message Hi in window=GlobalWindow
```

## License

These examples are provided under the Apache License 2.0.

## Issues

Report any issue to the GitHub issue tracker.
