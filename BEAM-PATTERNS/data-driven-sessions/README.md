# Data-driven Session Windows

This pattern is featured in the [Beam patterns section](https://beam.apache.org/documentation/patterns/custom-windows/#using-data-to-dynamically-set-session-window-gaps) and posted as an answer to a StackOverflow [question](https://stackoverflow.com/a/57030364/6121516).

Note that the Python one was not included in the official docs due to custom window merging not being available with FnAPI at the time.

## Example

The main idea is to be able to dynamically tune the inactivity gap to close a session according to the input data. This way, we can have different gaps for different users or events in the same pipeline.

---

# Java version:

We can adapt the code from [Session](https://github.com/apache/beam/blob/master/sdks/java/core/src/main/java/org/apache/beam/sdk/transforms/windowing/Sessions.java) windows to fit our use case.

Briefly, when records are windowed into sessions they get assigned to a window that begins at the element’s timestamp (unaligned windows) and adds the gap duration to the start to calculate the end. The `mergeWindows` function will then combine all overlapping windows per key resulting in an extended session.

We’ll need to modify the `assignWindows` function to create a window with a data-driven gap instead of a fixed duration. We can access the element through [`WindowFn.AssignContext.element()`](https://beam.apache.org/releases/javadoc/2.13.0/org/apache/beam/sdk/transforms/windowing/WindowFn.AssignContext.html#AssignContext--). The original assignment function is:


```java
public Collection<IntervalWindow> assignWindows(AssignContext c) {
  // Assign each element into a window from its timestamp until gapDuration in the
  // future.  Overlapping windows (representing elements within gapDuration of
  // each other) will be merged.
  return Arrays.asList(new IntervalWindow(c.timestamp(), gapDuration));
}
```

 The modified function will be:

```java
@Override
public Collection<IntervalWindow> assignWindows(AssignContext c) {
  // Assign each element into a window from its timestamp until gapDuration in the
  // future.  Overlapping windows (representing elements within gapDuration of
  // each other) will be merged.
  Duration dataDrivenGap;
  JSONObject message = new JSONObject(c.element().toString());

  try {
    dataDrivenGap = Duration.standardSeconds(Long.parseLong(message.getString(gapAttribute)));
  }
  catch(Exception e) {
    dataDrivenGap = gapDuration;
  }
  return Arrays.asList(new IntervalWindow(c.timestamp(), dataDrivenGap));
}
```

Note that we have added a couple extra things: 

* A **default** value for cases where the custom gap is not present in the data 
* A way to set the **attribute** from the main pipeline as a method of the custom windows.

The `withDefaultGapDuration` and `withGapAttribute` methods are:

```java
/** Creates a {@code DynamicSessions} {@link WindowFn} with the specified gap duration. */
public static DynamicSessions withDefaultGapDuration(Duration gapDuration) {
  return new DynamicSessions(gapDuration, "");
}

public DynamicSessions withGapAttribute(String gapAttribute) {
  return new DynamicSessions(gapDuration, gapAttribute);
}
```

We will also add a new field (`gapAttribute`) and constructor:

```java
public class DynamicSessions extends WindowFn<Object, IntervalWindow> {
  /** Duration of the gaps between sessions. */
  private final Duration gapDuration;

    /** Pub/Sub attribute that modifies session gap. */
  private final String gapAttribute;

  /** Creates a {@code DynamicSessions} {@link WindowFn} with the specified gap duration. */
  private DynamicSessions(Duration gapDuration, String gapAttribute) {
    this.gapDuration = gapDuration;
    this.gapAttribute = gapAttribute;
  }
```

Then, we can window our messages into the new custom sessions with:

```java
.apply("Window into sessions", Window.<String>into(DynamicSessions
  .withDefaultGapDuration(Duration.standardSeconds(10))
  .withGapAttribute("gap"))
```

In order to test this we’ll use a simple example with a controlled input. For our use case, we'll consider different needs for our users depending on the device where the app is running. Desktop users can be idle for long (allowing for longer sessions) whereas we only expect short-span sessions for our mobile users. We generate some mock data, where some messages contain the `gap` attribute and others omit it (gap window will resort to default for these ones):

```java
.apply("Create data", Create.timestamped(
            TimestampedValue.of("{\"user\":\"mobile\",\"score\":\"12\",\"gap\":\"5\"}", new Instant()),
            TimestampedValue.of("{\"user\":\"desktop\",\"score\":\"4\"}", new Instant()),
            TimestampedValue.of("{\"user\":\"mobile\",\"score\":\"-3\",\"gap\":\"5\"}", new Instant().plus(2000)),
            TimestampedValue.of("{\"user\":\"mobile\",\"score\":\"2\",\"gap\":\"5\"}", new Instant().plus(9000)),
            TimestampedValue.of("{\"user\":\"mobile\",\"score\":\"7\",\"gap\":\"5\"}", new Instant().plus(12000)),
            TimestampedValue.of("{\"user\":\"desktop\",\"score\":\"10\"}", new Instant().plus(12000)))
        .withCoder(StringUtf8Coder.of()))
```

Visually:

[![enter image description here][1]][1]

For the `desktop` user there are only two events separated 12 seconds away. No gap is specified so it will default to 10s and both scores will not be added up as they will belong to different sessions. 

The other user, `mobile`, has 4 events separated 2, 7 and 3 seconds respectively. None of the time separations is greater than the default gap, so with standard sessions all events would belong to a single session with added score of 18:

```java
user=desktop, score=4, window=[2019-05-26T13:28:49.122Z..2019-05-26T13:28:59.122Z)
user=mobile, score=18, window=[2019-05-26T13:28:48.582Z..2019-05-26T13:29:12.774Z)
user=desktop, score=10, window=[2019-05-26T13:29:03.367Z..2019-05-26T13:29:13.367Z)
```

However, with the new sessions, we specify a `gap` attribute of 5 seconds to those events. The third message comes 7 seconds after the second one thus being relegated to a different session now. The previous large session with score 18 will be split into two 9-point sessions:

```java
user=desktop, score=4, window=[2019-05-26T14:30:22.969Z..2019-05-26T14:30:32.969Z)
user=mobile, score=9, window=[2019-05-26T14:30:22.429Z..2019-05-26T14:30:30.553Z)
user=mobile, score=9, window=[2019-05-26T14:30:33.276Z..2019-05-26T14:30:41.849Z)
user=desktop, score=10, window=[2019-05-26T14:30:37.357Z..2019-05-26T14:30:47.357Z)
```

Tested with Java SDK 2.13.0

---

# Python version:

Analogously, we can extend the same approach to the Python SDK. The code for the `Sessions` class can be found [here](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/transforms/window.py). We’ll define a new `DynamicSessions` class. Inside the `assign` method we can access the processed record using `context.element` and modify the gap according to data.

Original:

```python
def assign(self, context):
  timestamp = context.timestamp
  return [IntervalWindow(timestamp, timestamp + self.gap_size)]
```

Extended:

```python
def assign(self, context):
  timestamp = context.timestamp

  try:
    gap = Duration.of(context.element[1][“gap”])
  except:
    gap = self.gap_size

  return [IntervalWindow(timestamp, timestamp + gap)]
```

If the input data contains a `gap` field it will use it to override the default gap size. In our pipeline code we just need to window events into `DynamicSessions` instead of the standard `Sessions`:

```python
'user_session_window'   >> beam.WindowInto(DynamicSessions(gap_size=gap_size),
                                             timestamp_combiner=window.TimestampCombiner.OUTPUT_AT_EOW)
```

With standard sessions the output is as follows:

```python
INFO:root:>> User mobile had 4 events with total score 18 in a 0:00:22 session
INFO:root:>> User desktop had 1 events with total score 4 in a 0:00:10 session
INFO:root:>> User desktop had 1 events with total score 10 in a 0:00:10 session
```

With our custom windowing mobile events are split into two different sessions:

```python
INFO:root:>> User mobile had 2 events with total score 9 in a 0:00:08 session
INFO:root:>> User mobile had 2 events with total score 9 in a 0:00:07 session
INFO:root:>> User desktop had 1 events with total score 4 in a 0:00:10 session
INFO:root:>> User desktop had 1 events with total score 10 in a 0:00:10 session
```

Tested with Python SDK 2.13.0


  [1]: https://i.stack.imgur.com/ybXvm.png

## License

These examples are provided under the Apache License 2.0.

## Issues

Report any issue to the GitHub issue tracker.
