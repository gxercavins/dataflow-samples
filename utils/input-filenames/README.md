# Input Filenames

Example on how to retrieve the suffix or file format of the file being read in order to do conditional processing. Originated from a [StackOverflow question](https://stackoverflow.com/questions/51685270/can-you-detect-object-file-name-using-cloud-dataflow/) that aimed at reading using a glob that might contain `CSV` and `XML` files that would require different steps down the pipeline.

## Example

One way we can do so is by using [`FileIO.match()`](https://beam.apache.org/documentation/sdks/javadoc/2.5.0/org/apache/beam/sdk/io/FileIO.html#match--), which will return [`ReadableFile`](https://beam.apache.org/documentation/sdks/javadoc/2.5.0/org/apache/beam/sdk/io/FileIO.ReadableFile.html) objects. The first thing we can do is to log one match so that we can see that the metadata contains the filename in the `resourceId` key-value pair:

> ReadableFile{metadata=Metadata{resourceId=gs://BUCKET_NAME/different/test1.csv,
> sizeBytes=30, isReadSeekEfficient=true}, compression=UNCOMPRESSED}

Looking at the `DifferentFileTypes.java` example we can design a ParDo that creates file key-value pairs where the key will be the file format derived from the suffix by subsetting after the last `.`:

''' java
PCollection<KV<String, String>> filenames = p.apply("Read files", FileIO.match().filepattern(input))
        .apply(FileIO.readMatches())
		.apply(ParDo.of(new DoFn<ReadableFile, KV<String, String>>() {
		    @ProcessElement
		    public void process(ProcessContext c) {
		    	// we'll output a KV where the file suffix is the key
		    	String filename = c.element().getMetadata().resourceId().toString();
		    	c.output(KV.of(filename.substring(filename.lastIndexOf('.') + 1), filename));
		    }
		}));
'''

The filename was retrieved from `ProcessContext.getMetadata().resourceId()` as explained before. In this case, `input` is the match pattern (i.e. `"gs://BUCKET_NAME/path/to/input/files/folder/*"`). 

Then we can now process the files differently down the pipeline according to the two different keys. In my example I just write a different string according to data type which can either be `CSV` or `XML`:

''' java
    filenames //
    .apply("Process according to type", ParDo.of(new DoFn<KV<String, String>, String>() {
    	@ProcessElement
    	public void processElement(ProcessContext c) {
    		String key = c.element().getKey();
    		String value = c.element().getValue();
    		if (key.equals("csv")) {c.output("CSV - " + value.substring(value.lastIndexOf('/') + 1));}
    		else {c.output("XML - " + value.substring(value.lastIndexOf('/') + 1));}
    	}
    }))//
    .apply(TextIO.write().to(output).withoutSharding());
'''

In my example the glob resolved to an input path that contained 5 files with mixed formats. The resulting output file after running the pipeline is:

'''
    CSV - test1.csv
    CSV - test2.csv
    XML - test2.xml
    XML - test1.xml
    CSV - test3.csv
'''

Alternatively, we can follow the approach in `FlattenTypes.java` where we can read each type of files into a PCollectionList by applying different match patterns. After that, we can apply the desired transformations which can be different for each format and then flatten the results.

''' java
    PCollectionList<KV<String,String>> pcl = PCollectionList.empty(p);

    pcl = pcl.and(
    	            p.apply("Read CSV files", FileIO.match().filepattern(input + "*.csv"))
    	                    .apply(FileIO.readMatches())
    						.apply(ParDo.of(new DoFn<ReadableFile, KV<String, String>>() {
    						    @ProcessElement
    						    public void process(ProcessContext c) {
    						    	c.output(KV.of("csv", c.element().getMetadata().resourceId().toString()));
    						    }
    						})))  	
    	            .and(
    	            p.apply("Read XML files", FileIO.match().filepattern(input + "*.xml"))
    	                    .apply(FileIO.readMatches())
    						.apply(ParDo.of(new DoFn<ReadableFile, KV<String, String>>() {
    						    @ProcessElement
    						    public void process(ProcessContext c){
    						    	c.output(KV.of("xml", c.element().getMetadata().resourceId().toString()));
    						    }
    						})));
    
    // combine/flatten all the PCollections together
    PCollection<KV<String, String>> flattenedPCollection = pcl.apply(Flatten.pCollections());
'''

In this case, `input` is the path to the folder containing the files (without wildcards or glob characters such as `*`)

Tested with Java 2.5.0 SDK.

## License

These examples are provided under the Apache License 2.0.

## Issues

Report any issue to the GitHub issue tracker.
