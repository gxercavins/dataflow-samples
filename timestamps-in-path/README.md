# Timestamps in path

This example, which originates from a [StackOverflow question](https://stackoverflow.com/questions/56454374/reading-files-and-folders-in-order-with-apache-beam/) aims to help with the following scenario:
* We have another process that writes hourly files to a folder structure such as `year/month/day/hour/*`
* We want to analyze all files corresponding to the same hour together
* This would look like a batch job candidate but new files can arrive and we want the same pipeline to keep watching

## Quickstart

You can use the provided `local.sh` and `cloud.sh` scripts (don't forget to add execution permissions `chmod +x *.sh`) for `DirectRunner` and `DataflowRunner`, as in:
``` bash
./local.sh <DATAFLOW_PROJECT_ID> <BUCKET_NAME>
./cloud.sh <DATAFLOW_PROJECT_ID> <BUCKET_NAME>
```

Alternatively, follow these steps:
* Set up [authentication](https://cloud.google.com/docs/authentication/) your preferred way 
* Set the `$PROJECT` and `$BUCKET` variables and run the Dataflow job with:
```bash
mvn -Pdataflow-runner compile -e exec:java \
 -Dexec.mainClass=com.dataflow.samples.ChronologicalOrder \
      -Dexec.args="--project=$PROJECT \
      --path=gs://$BUCKET/data/** \
      --stagingLocation=gs://$BUCKET/staging/ \
      --runner=DataflowRunner"
```

This code was tested with Java SDK 2.12.0.

## Example

The proposed approach is to add a timestamp to each element as derived from the file path.

As explained in [a previous answer](https://stackoverflow.com/a/54544945/6121516), with `FileIO` we can match continuously a file pattern. This will enable our job to backfill previous data and, after that, keep reading the GCS folder to watch if new files arrive. We'll provide a pattern such as `gs://BUCKET_NAME/data/**` expecting to match files such as `gs://BUCKET_NAME/data/year/month/day/hour/filename.extension`:

```java
p
	.apply(FileIO.match()
	.filepattern(inputPath)
	.continuously(
		// Check for new files every minute
		Duration.standardMinutes(1),
		// Never stop checking for new files
		Watch.Growth.<String>never()))
	.apply(FileIO.readMatches())
```

We can tailor at will the scanning frequency and specify a timeout if needed.

We'll feed the matched file to the next step. With `ReadableFile.getMetadata().resourceId()` we retrieve the full path and split it by `"/"` to build the corresponding timestamp. Take into account that the example rounds it to the hour and does not account for timezone correction. With `readFullyAsUTF8String` we'll load the whole file into a String (be careful if the whole file does not fit into memory, it is recommended to shard your input) and split it into lines. With `ProcessContext.outputWithTimestamp` we'll emit downstream a KV of filename and line with an associated timestamp derived from the path. The filename is not actually needed anymore but it will help to illustrate where each file comes from. Note that we're shifting timestamps "back in time" so this can mess up with the watermark heuristics and we'll get a message such as:

> Cannot output with timestamp 2019-03-17T00:00:00.000Z. Output timestamps must be no earlier than the timestamp of the current input (2019-06-05T15:41:29.645Z) minus the allowed skew (0 milliseconds). See the DoFn#getAllowedTimestampSkew() Javadoc for details on changing the allowed skew.

To circumvent this error we can set `getAllowedTimestampSkew` to `Long.MAX_VALUE` but we must take into account that overriding this value is deprecated. ParDo code:

```java
.apply("Add Timestamps", ParDo.of(new DoFn<ReadableFile, KV<String, String>>() {

	@Override
	public Duration getAllowedTimestampSkew() {
		return new Duration(Long.MAX_VALUE);
	}

	@ProcessElement
	public void processElement(ProcessContext c) {
		ReadableFile file = c.element();
		String fileName = file.getMetadata().resourceId().toString();
		String lines[];

		String[] dateFields = fileName.split("/");
		Integer numElements = dateFields.length;

		String hour = dateFields[numElements - 2];
		String day = dateFields[numElements - 3];
		String month = dateFields[numElements - 4];
		String year = dateFields[numElements - 5];

		String ts = String.format("%s-%s-%s %s:00:00", year, month, day, hour);
		Log.info(ts);
		
		try{
			lines = file.readFullyAsUTF8String().split("\n");
		
			for (String line : lines) {
				c.outputWithTimestamp(KV.of(fileName, line), new Instant(dateTimeFormat.parseMillis(ts)));
			}
		}

		catch(IOException e){
			Log.info("failed");
		}
	}}))
```

After that, we window into 1-hour `FixedWindows` and simply log the results:

```java
.apply(Window
	.<KV<String,String>>into(FixedWindows.of(Duration.standardHours(1)))
	.triggering(AfterWatermark.pastEndOfWindow())
	.discardingFiredPanes()
	.withAllowedLateness(Duration.ZERO))
.apply("Log results", ParDo.of(new DoFn<KV<String, String>, Void>() {
	@ProcessElement
	public void processElement(ProcessContext c, BoundedWindow window) {
		String file = c.element().getKey();
		String value = c.element().getValue();
		String eventTime = c.timestamp().toString();

		String logString = String.format("File=%s, Line=%s, Event Time=%s, Window=%s", file, value, eventTime, window.toString());
		Log.info(logString);
	}
}));
```

In my simple test it worked with `.withAllowedLateness(Duration.ZERO)` but depending on the order we might need to accept some late data. Setting a value too high will cause windows to be open for longer and more persistent state  will need to be stored.

I just uploaded a couple files:

```bash
gsutil cp file1 gs://$BUCKET/data/2019/03/17/00/
gsutil cp file2 gs://$BUCKET/data/2019/03/18/22/
```

Results:

[![enter image description here][1]][1]

This is just an example to get started and we might need to adjust windowing and triggering strategies, lateness, etc to suit different use cases.


  [1]: https://i.stack.imgur.com/1x9tA.png

## License

These examples are provided under the Apache License 2.0.

## Issues

Report any issue to the GitHub issue tracker.
