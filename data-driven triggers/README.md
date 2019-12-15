# Data-driven Triggers

Example written as an answer to this [StackOverflow question](https://stackoverflow.com/a/59175657/6121516) which includes the use case description. Briefly, we have a streaming pipeline that decomposes JSON arrays into its elements for parallel processing and then joins them back. The main issue is that processing time can vary and some elements will finish much faster than others. Downstream steps will emit results without waiting for all upstream operations to finish.

## Quickstart

Replace the correct Pub/Sub topic in `SODemoQuestion.java`:

```java
String topic = "projects/PROJECT_ID/topics/TOPIC";
```

Set up [authentication](https://cloud.google.com/docs/authentication/) your preferred way and run it locally with `local.sh`:

```bash
mvn compile -e exec:java \
 -Dexec.mainClass=com.dataflow.samples.SODemoQuestion \
      -Dexec.args="--runner=DirectRunner"
```

This code was tested with the Java 2.16.0 SDK and the `DirectRunner`.

## Example

Note that code is based on the StackOverflow question description. Main focus here is in adapting it to work with stateful pipelines and obtain the desired behavior.

In this case we assume that we know a priori the number of elements that conform the message so we define `NUM_ELEMENTS = 10`. Otherwise, we will know it when deserializing the message so we can embed it as part of the key (`id#num_elements`) and then have a third state variable with the number of elements per key and window.

The main idea of this approach is to keep track of the number of elements that we have seen so far for each particular key. 

In the stateful ParDo we define two state variables, one `BagState` with all integers seen and a `ValueState` to count the number of errors:

```java
// A state bag holding all elements seen for that key
@StateId("elements_seen")
private final StateSpec<BagState<Integer>> elementSpec =
      StateSpecs.bag();

// A state cell holding error count
@StateId("errors")
private final StateSpec<ValueState<Integer>> errorSpec =
      StateSpecs.value(VarIntCoder.of());
```

We process each element as usual but we don't output anything yet unless it's an error. In that case, we update the error counter before emitting the element to the `tagError` side output:

```java
errors.write(firstNonNull(errors.read(), 0) + 1);
is_error = true;
output.get(tagError).output(input);
```

Then, we update the count and, for successfully processed or skipped elements (i.e. `!is_error`), write the new observed element into the `BagState`:

```java
int count = firstNonNull(Iterables.size(state.read()), 0) + firstNonNull(errors.read(), 0);

if (!is_error) {
   state.add(input.getValue());
   count += 1;
}
```

After that, if the sum of successfully processed elements and errors is equal to `NUM_ELEMENTS` (we are simulating a data-driven trigger here), we are ready to flush all the items from the `BagState`:

```java
if (count >= NUM_ELEMENTS) {
   Iterable<Integer> all_elements = state.read();
   Integer key = input.getKey();

   for (Integer value : all_elements) {
      output.get(tagProcessed).output(KV.of(key, value));
   }
}
```

Note that here we can already group the values and emit just a single `KV<Integer, Iterable<Integer>>` instead. The `for` loop was used to avoid changing other steps downstream.

In order to test this, we can publish a message with `NUM_ELEMENTS` integers:

```bash
gcloud pubsub topics publish streamdemo --message "[1,2,3,4,5,6,7,8,9,10]"
```

With the original code we would get results for the first two elements that skip processing (without waiting for the rest):

```java
INFO: DONE: [4,8]
```

And now, after incorporating the State API, we get:

```java
INFO: DONE: [1,2,3,4,5,6,8,9,10]
```

Note that element `7` is not present in the output as is the one that simulates errors (but it's still counted to know that the message is now complete).

## License

This example is provided under the Apache License 2.0.

## Issues

Report any issue to the GitHub issue tracker.
