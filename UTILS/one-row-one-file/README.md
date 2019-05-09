## One row, one file

Quick example showing how to control the number of output files with `FileIO.writeDynamic`. Written as an answer to a [StackOverflow question](https://stackoverflow.com/questions/55887072/is-there-a-way-to-write-one-file-for-each-record-with-apache-beam-fileio/55890045#55890045).

## Example

We will use [`FileIO.writeDynamic`][1] and specify in the `.by` clause how do we want to write them. For example, if we have unique keys for each record we can use `.by(KV::getKey)` and each element will be written to a separate file. Otherwise, the criterion can be the hash of the row plus the timestamp (to ensure uniqueness), etc. also we can tune `.withNaming` at will.

As an example, we create four KV pairs with unique keys and write each value to a different output file:

```java
p.apply("Create Data", Create.of(KV.of("one", "this is row 1"), KV.of("two", "this is row 2"), KV.of("three", "this is row 3"), KV.of("four", "this is row 4")))
 .apply(FileIO.<String, KV<String, String>>writeDynamic()
    .by(KV::getKey)
    .withDestinationCoder(StringUtf8Coder.of())
    .via(Contextful.fn(KV::getValue), TextIO.sink())
    .to(output)
    .withNaming(key -> FileIO.Write.defaultNaming("file-" + key, ".txt")));
```

This will write the four elements into four files:

```bash
$ mvn compile -e exec:java \
 -Dexec.mainClass=com.dataflow.samples.OneRowOneFile \
      -Dexec.args="--project=$PROJECT \
      --output="output/" \
      --runner=DirectRunner"

$ ls output/
file-four-00001-of-00003.txt  file-one-00002-of-00003.txt  file-three-00002-of-00003.txt  file-two-00002-of-00003.txt
$ cat output/file-four-00001-of-00003.txt 
this is row 4
```

  [1]: https://beam.apache.org/releases/javadoc/2.12.0/org/apache/beam/sdk/io/FileIO.html#writeDynamic--


## License

These examples are provided under the Apache License 2.0.

## Issues

Report any issue to the GitHub issue tracker.
