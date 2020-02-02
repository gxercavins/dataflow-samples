## Flatten Side Outputs

How to get, into a single PCollection, elements from two different side outputs. Written as an answer to a [StackOverflow question](https://stackoverflow.com/a/60021419/6121516).

## Example

We can use the `Flatten` transform ([Beam guide][1]) to merge different PCollections into a single one. For our example we'll create a couple dummy lines such as:

```java
final List<String> LINES = Arrays.asList(
    "good line",
    "bad line"
);
```

that will be classified into `OK` or `NOTOK` lines:

```java
PCollectionTuple myPCollection = p
    .apply("Create Data", Create.of(LINES))
    .apply("Split OK/NOTOK", ParDo.of(new ValidateFn())
                .withOutputTags(OK, TupleTagList.of(NOTOK)));
```

where the criterion employed in `ValidateFn` will be to check if it contains the word `good`:

```java
static class ValidateFn extends DoFn<String, String> {
    @ProcessElement
    public void processElement(ProcessContext c, MultiOutputReceiver r) {
        String line = c.element();
        
        if (line.contains("good")) {
            r.get(OK).output(line);
        }
          
        else {
            r.get(NOTOK).output(line);
        }
    }
}
```

and we have designated our side outpu tags and PCollections as:

```java
public static final TupleTag<String> OK = new TupleTag<String>(){};
public static final TupleTag<String> NOTOK = new TupleTag<String>(){};

PCollection<String> okResults = myPCollection.get(OK);
PCollection<String> notOkResults = myPCollection.get(NOTOK);
```

Then we can use `okResults` and `notOkResults` to apply further processing according to the element validation. Using `PCollectionList` and `Flatten` we are able to retrieve both.

```java
PCollectionList<String> pcl = PCollectionList.empty(p);
pcl = pcl.and(okResults).and(notOkResults);
PCollection<String> allResults = pcl.apply(Flatten.pCollections());
```

In this case `allResults` will contain **both** `OK` and `NOTOK` elements:

```java
Feb 01, 2020 10:42:24 PM org.apache.beam.examples.AllSideOutputs$5 processElement
INFO: All elements: bad line
Feb 01, 2020 10:42:24 PM org.apache.beam.examples.AllSideOutputs$5 processElement
INFO: All elements: good line
Feb 01, 2020 10:42:24 PM org.apache.beam.examples.AllSideOutputs$3 processElement
INFO: Ok element: good line
Feb 01, 2020 10:42:24 PM org.apache.beam.examples.AllSideOutputs$4 processElement
INFO: Not Ok element: bad line
```

Tested with 2.17.0 SDK and the `DirectRunner`.Full code in `FlattenSideOutputs.java`.


  [1]: https://beam.apache.org/documentation/programming-guide/#core-beam-transforms


## License

These examples are provided under the Apache License 2.0.

## Issues

Report any issue to the GitHub issue tracker.
