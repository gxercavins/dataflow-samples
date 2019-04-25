# Global Options

This example shows how to define custom options in a template, pass them at runtime and make them accessible globally so we can use them inside other functions such as a ParDo.

## Quickstart

Set up [authentication](https://cloud.google.com/docs/authentication/) your preferred way. You can use the provided `stage.sh` and `execute.sh` scripts to stage and launch the template, correspondingly. Edit the variables -such as Project ID or Bucket name- in the top section of the scripts.

This code was tested with version 2.5.0 of the Java SDK.

## Example

This was originally an answer to a StackOverflow [question](https://stackoverflow.com/questions/52434647/how-to-access-pipeline-options-within-par-do-transforms/). Base code was provided by user `nomadic_squirrel` and this example only intends to explain how to ensure those options are globally available. Code is included in the `OptionsInParDo.java` file.

The main idea is to use [`ValueProvider`](https://beam.apache.org/documentation/sdks/javadoc/2.5.0/org/apache/beam/sdk/options/ValueProvider.html) so that we can pass a different `orgId` parameter at runtime each time we execute the templated job.

```java
public interface MyOptions extends PipelineOptions {    
     @Description("Org Id")
     @Default.String("123-984-a")
     ValueProvider<String> getOrgId();
     void setOrgId(ValueProvider<String> orgID);   
}
```

Then read it from options with:

```java
ValueProvider<String> orgId = options.getOrgId();
```

However, when we read the value from a ParDo outside of the main function we only get a `null` value. For this to be accessible within the ParDo you can pass it as a parameter to the constructor such as the example in the [docs](https://cloud.google.com/dataflow/docs/templates/creating-templates#using-valueprovider-in-your-pipeline-options):

```java
someDataRows.apply( "Package into a list", ParDo.of( new CustomFn(orgId)));
```

where `CustomFn`'s constructor takes it as an arguments and stores it in a `ValueProvider` so that it's accessible from within the ParDo. Notice that we now need to use `orgId.get()` as it is a `ValueProvider`:

```java
static class CustomFn extends DoFn<String, String> {
	// access options from wihtin the ParDo
	ValueProvider<String> orgId;
	public CustomFn(ValueProvider<String> orgId) {
	    this.orgId = orgId;
	}

	@ProcessElement
	public void processElement( ProcessContext c ) {
	  LOG.info( "Hello? " );
	  LOG.info( "ORG ID: " + orgId.get() );
	}
}
```

Now we can stage the template (`stage.sh`) and call it (`execute.sh`) specifying the desired options as key/value pairs and the `--parameters` flag:

```bash
gcloud dataflow jobs run $JOB_NAME \
	--gcs-location gs://$BUCKET/templates/$TEMPLATE_NAME\
	--parameters orgId=jomama47
```

The result now shows the correct `orgId` value when logged from `CustomFn`:

[![correct orgId in step logs][1]][1]


  [1]: https://i.stack.imgur.com/8q6hE.png

## License

These examples are provided under the Apache License 2.0.

## Issues

Report any issue to the GitHub issue tracker.
