# Session windows in Python

One of the more interesting type of windows is sessions. With sessions we are merging together all consecutive events from the same user and closing the windows only when we detect a certain gap of inactivity. This makes them a natural fit for applications such as mobile games or e-commerce websites. They are data-driven as the length depends on the actual data/events and captures the asymmetry of each user's pattern.

Here we'll present a Python example written as an answer to a [StackOverflow question](https://stackoverflow.com/questions/55261957/session-windows-in-apache-beam-with-python).

## Quickstart

Set up [authentication](https://cloud.google.com/docs/authentication/) your preferred way and install the Beam Python package (use of `virtualenv` is recommended): `pip install apache-beam[gcp]==2.9.0`.

The script can be tested locally with: `python sessions.py`.

To run on Google Cloud Platform set up the `PROJECT` and `BUCKET` env variables and execute: `python sessions.py --runner DataflowRunner --temp_location gs://$BUCKET/temp --project $PROJECT`.

This code was tested with `apache-beam==2.9.0`.

## Example

To showcase session windowing we will first create in-memory dummy data (it's easier and faster to reproduce without a source such as Pub/Sub). We'll have events belonging to two different users but first we'll focus on the activity of a single one:

```python
data = [{'user_id': 'Thanos', 'value': 'event_{}'.format(event), 'timestamp': time.time() + 2**event} for event in range(5)]
```

All five events will have the same key or `user_id` but they will "arrive" sequentially 1, 2, 4 and 8 seconds apart from each other. As we'll use a `session_gap` of 5 seconds we expect the first 4 elements to be merged into the same session. The 5th event will take 8 extra seconds after the 4th one so it has to be relegated to the next session (gap > 5s). 

We use `beam.Create(data)` to initialize the pipeline and `beam.window.TimestampedValue` to assign the "fake" timestamps. Again, we are just simulating streaming behavior with this. After that, we create the key-value pairs thanks to the `user_id` field, we window into `window.Sessions` and, we add the missing `beam.GroupByKey()` step. Finally, we log the results. The pipeline now looks like this:

```python
events = (p
  | 'Create Events' >> beam.Create(data) \
  | 'Add Timestamps' >> beam.Map(lambda x: beam.window.TimestampedValue(x, x['timestamp'])) \
  | 'keyed_on_user_id'      >> beam.Map(lambda x: (x['user_id'], x))
  | 'user_session_window'   >> beam.WindowInto(window.Sessions(session_gap),
                                             timestamp_combiner=window.TimestampCombiner.OUTPUT_AT_EOW) \
  | 'Group' >> beam.GroupByKey()
  | 'debug_printer'         >> beam.ParDo(DebugPrinter()))
```

As per [another question](https://stackoverflow.com/questions/55219481/apache-beam-per-user-session-windows-are-unmerged/), if we test this without grouping by key we get the events are unmerged:

```python
INFO:root:>>> Received event_0 1554117323.0 with window=[1554117323.0, 1554117328.0)
INFO:root:>>> Received event_1 1554117324.0 with window=[1554117324.0, 1554117329.0)
INFO:root:>>> Received event_2 1554117326.0 with window=[1554117326.0, 1554117331.0)
INFO:root:>>> Received event_3 1554117330.0 with window=[1554117330.0, 1554117335.0)
INFO:root:>>> Received event_4 1554117338.0 with window=[1554117338.0, 1554117343.0)
```

But, after adding it, the windows now work as expected. Events 0 to 3 are merged together in an extended 12s session window. Event 4 belongs to a separate 5s session.

```python
INFO:root:>>> Received event_0 1554118377.37 with window=[1554118377.37, 1554118389.37)
INFO:root:>>> Received event_1 1554118378.37 with window=[1554118377.37, 1554118389.37)
INFO:root:>>> Received event_3 1554118384.37 with window=[1554118377.37, 1554118389.37)
INFO:root:>>> Received event_2 1554118380.37 with window=[1554118377.37, 1554118389.37)
INFO:root:>>> Received event_4 1554118392.37 with window=[1554118392.37, 1554118397.37)
```

Focusing now on both users (with `beam.Create(user1_data + user2_data)`) we can add an `AnalyzeSession` after the GBK:

```python
      | 'Group' >> beam.GroupByKey() \
      | 'analyze_session'         >> beam.ParDo(AnalyzeSession()))
```

We can log the results with:

```python
class AnalyzeSession(beam.DoFn):
  """Prints per session information"""
  def process(self, element, window=beam.DoFn.WindowParam):
    logging.info(element)
    yield element
```

which simply prints out the result of each session:

```python
INFO:root:('Groot', [{'timestamp': 1554203778.904401, 'user_id': 'Groot', 'value': 'event_0'}, {'timestamp': 1554203780.904401, 'user_id': 'Groot', 'value': 'event_1'}])
INFO:root:('Groot', [{'timestamp': 1554203786.904402, 'user_id': 'Groot', 'value': 'event_2'}])
INFO:root:('Thanos', [{'timestamp': 1554203792.904399, 'user_id': 'Thanos', 'value': 'event_4'}])
INFO:root:('Thanos', [{'timestamp': 1554203784.904398, 'user_id': 'Thanos', 'value': 'event_3'}, {'timestamp': 1554203777.904395, 'user_id': 'Thanos', 'value': 'event_0'}, {'timestamp': 1554203778.904397, 'user_id': 'Thanos', 'value': 'event_1'}, {'timestamp': 1554203780.904398, 'user_id': 'Thanos', 'value': 'event_2'}])
```

We can do more meaningful stuff by reducing the aggregated events on a per-session level like counting the number of events or session duration:

```python
class AnalyzeSession(beam.DoFn):
  """Prints per session information"""
  def process(self, element, window=beam.DoFn.WindowParam):
    user = element[0]
    num_events = str(len(element[1]))
    window_end = window.end.to_utc_datetime()
    window_start = window.start.to_utc_datetime()
    session_duration = window_end - window_start

    logging.info(">>> User %s had %s event(s) in %s session", user, num_events, session_duration)

    yield element
```

which, for our example, it will output the following:

```python
INFO:root:>>> User Groot had 2 event(s) in 0:00:07 session
INFO:root:>>> User Groot had 1 event(s) in 0:00:05 session
INFO:root:>>> User Thanos had 4 event(s) in 0:00:12 session
INFO:root:>>> User Thanos had 1 event(s) in 0:00:05 session
```

Two additional things worth mentioning. The first one is that, even if running this locally in a single machine with the DirectRunner, records can come unordered (`event_3` is processed before `event_2` in my case). This is done on purpose to simulate distributed processing as documented [here][1].

The last one is that if we get a stack trace like this:

```python
TypeError: Cannot convert GlobalWindow to apache_beam.utils.windowed_value._IntervalWindowBase [while running 'Write Results/Write/WriteImpl/WriteBundles']
```

we can solve it by downgrading from 2.10.0/2.11.0 SDK to 2.9.0.


  [1]: https://beam.apache.org/documentation/runners/direct/


## License

This example is provided under the Apache License 2.0.

## Issues

Report any issue to the GitHub issue tracker.
