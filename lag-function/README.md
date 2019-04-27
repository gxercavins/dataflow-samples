# Simulate LAG function

The main idea for this example, written as an answer to a [StackOverflow question](https://stackoverflow.com/questions/55279946/dataflow-look-up-a-previous-event-in-an-event-stream), is to be able to compare an event with its equivalent one in the previous window (i.e. trying to simulate a LAG over one period function).

## Quickstart

Set up [authentication](https://cloud.google.com/docs/authentication/) your preferred way and install the Beam Python package (use of `virtualenv` is recommended): `pip install apache-beam[gcp]==2.9.0`.

The script can be tested locally with (replace the variables): `python lag.py --input projects/$PROJECT/topics/$TOPIC --output --$PATH`.

To run on Google Cloud Platform execute: `python lag.py --runner DataflowRunner --temp_location gs://$BUCKET/temp --project $PROJECT --input projects/$PROJECT/topics/$TOPIC --output --gs://$BUCKET/output`.

This code was tested with the Direct Runner and `apache-beam==2.9.0`.

## Example

The first idea would be to consider a stateful DoFn but, as explained in the [docs][1], state is maintained per key and window. Therefore, in the processing context we can't access values from a previous windows. To attain our objective we will need to implement a more complex pipeline design. One possible idea would be to duplicate the messages in a ParDo:

* We'll emit them unmodified to the main output
* At the same time, we'll send them to a side output with exactly one-period lag

To accomplish the latter, we have to add the duration of a window (`WINDOW_SECONDS`) to the current element's timestamp:

```python
class DuplicateWithLagDoFn(beam.DoFn):

  def process(self, element, timestamp=beam.DoFn.TimestampParam):
    # Main output gets unmodified element
    yield element
    # The same element is emitted to the side output with a 1-window lag added to timestamp
    yield beam.pvalue.TaggedOutput('lag_output', beam.window.TimestampedValue(element, timestamp + WINDOW_SECONDS))
```

We call the `DuplicateWithLagDoFn` function specifying the correct tags for each output:

```python
beam.ParDo(DuplicateWithLagDoFn()).with_outputs('lag_output', main='main_output')
```

and then apply the same windowing scheme to both, co-group by key, etc.

```python
windowed_main = results.main_output | 'Window main output' >> beam.WindowInto(window.FixedWindows(WINDOW_SECONDS))
windowed_lag = results.lag_output | 'Window lag output' >> beam.WindowInto(window.FixedWindows(WINDOW_SECONDS))

merged = (windowed_main, windowed_lag) | 'Join Pcollections' >> beam.CoGroupByKey()
```

Finally, we can access both values (old and new) inside the same ParDo:

```python
class CompareDoFn(beam.DoFn):

  def process(self, element):
    logging.info("Combined with previous vale: {}".format(element))
 
    try:
      old_value = int(element[1][1][0].split(',')[1])
    except:
      old_value = 0

    try:
      new_value = int(element[1][0][0].split(',')[1])
    except:
      new_value = 0

    logging.info("New value: {}, Old value: {}, Difference: {}".format(new_value, old_value, new_value - old_value))
    return (element[0], new_value - old_value)
```

To test this we run the pipeline with the Direct Runner and, in a separate shell, we publish two messages more than 10 seconds apart (as in our case `WINDOW_SECONDS` was set to 10s):

```bash
gcloud pubsub topics publish lag --message="test,120"
sleep 12
gcloud pubsub topics publish lag --message="test,40"
```

And the job output shows the expected difference values for each window:

```python
INFO:root:New message: (u'test', u'test,120')
INFO:root:Combined with previous vale: (u'test', ([u'test,120'], []))
INFO:root:New value: 120, Old value: 0, Difference: 120
INFO:root:New message: (u'test', u'test,40')
INFO:root:Combined with previous vale: (u'test', ([u'test,40'], [u'test,120']))
INFO:root:New value: 40, Old value: 120, Difference: -80
INFO:root:Combined with previous vale: (u'test', ([], [u'test,40']))
INFO:root:New value: 0, Old value: 40, Difference: -40
```

Take into account performance considerations as we are duplicating elements but it makes sense if we need to have values available during two windows before "dropping" them.


  [1]: https://beam.apache.org/blog/2017/02/13/stateful-processing.html


## License

This example is provided under the Apache License 2.0.

## Issues

Report any issue to the GitHub issue tracker.
