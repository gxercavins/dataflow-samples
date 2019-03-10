# Regex patterns
## Associate each matched filename with the corresponding input path pattern

Use case is we have a PCollection with different patterns and its corresponding tags such as:
```java
KV("gs://BUCKET/sales/*", "Sales"),
KV("gs://BUCKET/events/*", "Events")
```
We want to be able to apply those tags to each matched filename:
```java
KV("gs://BUCKET/sales/sales1.csv", "Sales"),
KV("gs://BUCKET/sales/sales2.csv", "Sales"),
KV("gs://BUCKET/events/events1.csv", "Events"),
KV("gs://BUCKET/events/events2.csv", "Events")
```
Number and path of patterns is dynamic (can vary between of executions) so it can't be hard-coded in the pipeline. This example was originally written as an answer to [this StackOverflow question][1].

## Quickstart

You can use the provided `run.sh` script (don't forget to add execution permissions `chmod +x run.sh`) as in:
```bash
./run.sh <DATAFLOW_PROJECT_ID> <BUCKET_NAME>
```

Alternatively, follow these steps:
* Set up [authentication](https://cloud.google.com/docs/authentication/) your preferred way 
* Set the `$PROJECT` and `$BUCKET`:
```bash
mvn compile -e exec:java \
 -Dexec.mainClass=com.dataflow.samples.RegexFileIO \
      -Dexec.args="--project=$PROJECT \
      --bucket=$BUCKET \
      --runner=DirectRunner"
```

The script will try to match the paths `gs://BUCKET-NAME/sales/*` and `gs://BUCKET-NAME/events/*`

This code was tested with Java SDK 2.5.0.

## Example

[`MatchResult.Medata`][2] contains the `resourceId` but not the GCS path (with wildcards) it matched. To achieve the desired outcome we can use side inputs.

We first create the following `filesAndSources` as an example (in practice it can be specified at runtime):

```java
PCollection<KV<String, String>> filesAndSources = p.apply("Create file pattern and source pairs",
	Create.of(KV.of("gs://" + Bucket + "/sales/*", "Sales"),
              KV.of("gs://" + Bucket + "/events/*", "Events")));
```

I materialize this into a side input (in this case as `Map`). The key will be the glob pattern converted into a regex one (thanks to [this answer][3]) and the value will be the source string:

```java
    final PCollectionView<Map<String, String>> regexAndSources =
    filesAndSources.apply("Glob pattern to RegEx", ParDo.of(new DoFn<KV<String, String>, KV<String, String>>() {
      @ProcessElement
      public void processElement(ProcessContext c) {
        String regex = c.element().getKey();
    
        StringBuilder out = new StringBuilder("^");
        for(int i = 0; i < regex.length(); ++i) {
            final char ch = regex.charAt(i);
            switch(ch) {
                case '*': out.append(".*"); break;
                case '?': out.append('.'); break;
                case '.': out.append("\\."); break;
                case '\\': out.append("\\\\"); break;
                default: out.append(ch);
            }
        }
        out.append('$');
        c.output(KV.of(out.toString(), c.element().getValue()));
    }})).apply("Save as Map", View.asMap());
```

Then, after reading the filenames we can use the side input to parse each path to see which is the matching pattern/source pair:

```java
filesAndSources
  .apply("Get file names", Keys.create()) 
  .apply(FileIO.matchAll())
  .apply(FileIO.readMatches())
  .apply(ParDo.of(new DoFn<ReadableFile, KV<String, String>>() {
      @ProcessElement
      public void processElement(ProcessContext c) {
        ReadableFile file = c.element();
        String fileName = file.getMetadata().resourceId().toString();

        Set<Map.Entry<String,String>> patternSet = c.sideInput(regexAndSources).entrySet();    

        for (Map.Entry< String,String> pattern:patternSet) 
        { 
            if (fileName.matches(pattern.getKey())) {
              String source = pattern.getValue();
              c.output(KV.of(fileName, source));
       		}
       	}
     }}).withSideInputs(regexAndSources))
```

Note that the regex conversion is done before materializing the side input instead of here to avoid duplicate work.

The output, as expected according to our problem description:

```java
Feb 24, 2019 10:44:05 PM org.apache.beam.sdk.io.FileIO$MatchAll$MatchFn process
INFO: Matched 2 files for pattern gs://REDACTED/events/*
Feb 24, 2019 10:44:05 PM org.apache.beam.sdk.io.FileIO$MatchAll$MatchFn process
INFO: Matched 2 files for pattern gs://REDACTED/sales/*
Feb 24, 2019 10:44:05 PM com.dataflow.samples.RegexFileIO$3 processElement
INFO: key=gs://REDACTED/sales/sales1.csv, value=Sales
Feb 24, 2019 10:44:05 PM com.dataflow.samples.RegexFileIO$3 processElement
INFO: key=gs://REDACTED/sales/sales2.csv, value=Sales
Feb 24, 2019 10:44:05 PM com.dataflow.samples.RegexFileIO$3 processElement
INFO: key=gs://REDACTED/events/events1.csv, value=Events
Feb 24, 2019 10:44:05 PM com.dataflow.samples.RegexFileIO$3 processElement
INFO: key=gs://REDACTED/events/events2.csv, value=Events
```

  [1]: https://stackoverflow.com/questions/54838908/how-to-add-additional-field-to-beam-fileio-matchall-result
  [2]: https://beam.apache.org/releases/javadoc/2.9.0/org/apache/beam/sdk/io/fs/MatchResult.Metadata.html
  [3]: https://stackoverflow.com/a/45321638/6121516

## License

These examples are provided under the Apache License 2.0.

## Issues

Report any issue to the GitHub issue tracker.
