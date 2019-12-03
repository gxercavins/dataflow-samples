# Assign Sessons

We have a batch job where our input data contains two custom classes: one that represents timestamped events and another one that collects all possible sessions with a start and finish timestamp. We want to assign each incoming event into the correct session.

Example written as an answer to a [StackOverflow question](https://stackoverflow.com/a/58739990/6121516).

## Quickstart

Set up [authentication](https://cloud.google.com/docs/authentication/) your preferred way and run it locally with:

```bash
mvn compile -e exec:java \
 -Dexec.mainClass=com.dataflow.samples.AssignSessions \
      -Dexec.args="--runner=DirectRunner"
```

This code was tested with the Java 2.16.0 SDK and the `DirectRunner`.

## Example

We can go through all the possible `Sessions` first, build a list and save it as a `PCollectionView`. Then, we parse each `Event` and decide into which one of the previous `Sessions` would it fall.

We will define the classes and constructors as follows:

```java
@DefaultCoder(AvroCoder.class)
public static class Session {
    String id;
    long start; 
    long stop;

    public Session(String id, long start, long stop) {
    this.id = id;
    this.start = start;
    this.stop = stop;
  }

    public Session() {
        // for serialization only
    }
}

@DefaultCoder(AvroCoder.class)
public static class Event {
  String id;
    long timestamp;

    public Event(String id, long timestamp) {
    this.id = id;
    this.timestamp = timestamp;
  }

    public Event() {
        // for serialization only
    }
}
```

and build our test data such as:

```java
// Example sessions data
final List<Session> sessionList = Arrays.asList(
    new Session("s1", 0L, 100L),
    new Session("s2", 100L, 200L),
    new Session("s3", 200L, 300L)
);

// Example event data
final List<Event> eventList = Arrays.asList(
    new Event("e1", 20L),
    new Event("e2", 60L),
    new Event("e3", 120L),
    new Event("e4", 160L),
    new Event("e5", 210L),
    new Event("e6", 290L)            
);
```

Events `e1` and `e2` should fall into Session `s1`. Events `e3` and `e4` go into `s2`. The last two remaining events, `e5` and `e6`, fit right into `s3`.

Then, we'll build our `PCollectionView` with all possible sessions:

```java
// create PCollectionView from sessions
final PCollectionView<List<Session>> sessionPC = p
    .apply("Create Sessions", Create.of(sessionList))
    .apply("Save as List", View.asList());
```

and, for each `Event`, we'll check in the `AssignFn` ParDo in which `Session` should the `Event` fall in:

```java
public static class AssignFn extends DoFn<Event, KV<Session, Event>> {  

    final PCollectionView<List<Session>> sessionPC;
    
    public AssignFn(PCollectionView<List<Session>> TagsideInput) {
        this.sessionPC = TagsideInput;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
        Event event = c.element();

        // get side input with all possible Sessions
        List<Session> sessions = c.sideInput(sessionPC);

        // where does the Event fall in?
        for (Session session:sessions) { 
            if (event.timestamp >= session.start && event.timestamp <= session.stop) {
                c.output(KV.of(session, event));
                break;
            }
        }
    }
}
```

The main pipeline structure is:

```java
p
    .apply("Create Events", Create.of(eventList))
    .apply("Assign Sessions", ParDo.of(new AssignFn(sessionPC))
        .withSideInputs(sessionPC))
    .apply("Group By Key", GroupByKey.<Session,Event>create())
    .apply("Log Grouped Results", ParDo.of(new LogFn()));
```

Note that, after the `Session` assignment, we apply a `GroupByKey` operation to have the desired output in the form `KV<Session, Iterable<Event>>`. 

`LogFn` will be used only to verify content:

```java
public static class LogFn extends DoFn<KV<Session, Iterable<Event>>, KV<Session, Iterable<Event>>> {  

    @ProcessElement
    public void processElement(ProcessContext c) {
        Session session = c.element().getKey();
        Iterable<Event> events = c.element().getValue();
        StringBuilder str = new StringBuilder(); 
      
        // print session info
        str.append(String.format("\nSession id=%s, start=%d, stop=%d", session.id, session.start, session.stop));

        // print each event info
        for (Event event:events) { 
            str.append(String.format("\n---Event id=%s, timestamp=%d", event.id, event.timestamp));
        }

        LOG.info(str.toString());

        c.output(c.element());
    }
}
```

We get the expected output (as previously described):

```java
Nov 07, 2019 12:36:22 AM com.dataflow.samples.AssignSessions$LogFn processElement
INFO: 
Session id=s2, start=100, stop=200
---Event id=e3, timestamp=120
---Event id=e4, timestamp=160
Nov 07, 2019 12:36:22 AM com.dataflow.samples.AssignSessions$LogFn processElement
INFO: 
Session id=s1, start=0, stop=100
---Event id=e1, timestamp=20
---Event id=e2, timestamp=60
Nov 07, 2019 12:36:22 AM com.dataflow.samples.AssignSessions$LogFn processElement
INFO: 
Session id=s3, start=200, stop=300
---Event id=e6, timestamp=290
---Event id=e5, timestamp=210
```

## License

This example is provided under the Apache License 2.0.

## Issues

Report any issue to the GitHub issue tracker.
