## MapElements example

Quick example on how to use the `MapElements` transform as per this [StackOverflow question](https://stackoverflow.com/questions/56720785/how-can-i-dynamically-add-field-in-mapelements-in-apache-beam/). 2.13.0 docs can be found [here](https://beam.apache.org/releases/javadoc/2.13.0/org/apache/beam/sdk/transforms/MapElements.html).

## Quickstart

Set up [authentication](https://cloud.google.com/docs/authentication/) your preferred way.

Execute the `run.sh` script with: `$ ./run.sh PROJECT_ID`.

Tested with DirectRunner and Java SDK 2.13.0

## Example

The use case is to add an additional field to every element being processed using the `mapElements` transform. For example, if the input is a 1-element PCollection with a list containing the `blah` string, we want to extend the output list to incorporate the `Its weekend!` string (apart from the original one).

```java
INPUT: ['blah']
OUTPUT: ['blah', 'Its weekend!']
```

The `via` method of `mapElements` expects one of the following: `InferableFunction`, `SimpleFunction`, `ProcessFunction`, `SerializableFunction`, `Contextful`. Three ways to do so:

```java
// via ProcessFunction
PCollection p1 = p.apply(Create.of(LINES))
    .apply(MapElements.into(TypeDescriptors.lists(TypeDescriptors.strings()))
                      .via((String word) -> (Arrays.asList(word, "Its weekend!"))))
    .apply(ParDo.of(new PrintResultsFn()));

// via in-line SimpleFunction
PCollection p2 = p.apply(Create.of(LINES))
    .apply(MapElements.via(new SimpleFunction<String, List<String>>() {
      public List<String> apply(String word) {
          return Arrays.asList(word, "Its weekend!");
      }}))
    .apply(ParDo.of(new PrintResultsFn()));

// via AddFieldFn class 
PCollection p3 = p.apply(Create.of(LINES))
  .apply(MapElements.via(new AddFieldFn()))
  .apply(ParDo.of(new PrintResultsFn()));
```

where `AddFieldFn` is:

```java
// define AddFieldFn extending from SimpleFunction and overriding apply method
static class AddFieldFn extends SimpleFunction<String, List<String>> {
    @Override
    public List<String> apply(String word) {
        return Arrays.asList(word, "Its weekend!");
    }
}
```

and `PrintResultsFn` is used just to verify the results:

```java
// just print the results
static class PrintResultsFn extends DoFn<List<String>, Void> {
    @ProcessElement
    public void processElement(@Element List<String> words) {
        Log.info(Arrays.toString(words.toArray()));
    }
}
```

Script execution prompts the expected output:

```java
Jun 23, 2019 8:00:03 PM com.dataflow.samples.SampleTextIO$PrintResultsFn processElement
INFO: [blah, Its weekend!]
Jun 23, 2019 8:00:03 PM com.dataflow.samples.SampleTextIO$PrintResultsFn processElement
INFO: [blah, Its weekend!]
Jun 23, 2019 8:00:03 PM com.dataflow.samples.SampleTextIO$PrintResultsFn processElement
INFO: [blah, Its weekend!]
```

## License

These examples are provided under the Apache License 2.0.

## Issues

Report any issue to the GitHub issue tracker.
