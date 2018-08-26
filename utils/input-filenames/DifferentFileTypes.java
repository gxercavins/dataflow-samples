package com.dataflow.samples;

import java.util.ArrayList;
import java.util.List;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileIO.ReadMatches;
import org.apache.beam.sdk.io.FileIO.Match;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.TextIO.Write;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;


public class DifferentFileTypes {

	public static interface MyOptions extends DataflowPipelineOptions {
		@Description("Output path: gs://BUCKET_NAME/path/to/destination/file.suffix")
		String getOutput();

		void setOutput(String s);

		@Description("Match pattern: gs://BUCKET_NAME/path/to/input/files/folder/*")
		String getInput();
		
		void setInput(String s);
	}

	@SuppressWarnings("serial")
	public static void main(String[] args) {
		MyOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(MyOptions.class);
		Pipeline p = Pipeline.create(options);

		String input = options.getInput();
		String output = options.getOutput();

		// combine/flatten the PCollections together
		PCollection<KV<String, String>> filenames = p.apply("Read files", FileIO.match().filepattern(input))
                .apply(FileIO.readMatches())
				.apply(ParDo.of(new DoFn<ReadableFile, KV<String, String>>() {
				    @ProcessElement
				    public void process(ProcessContext c) {
				    	String filename = c.element().getMetadata().resourceId().toString();
				    	c.output(KV.of(filename.substring(filename.lastIndexOf('.') + 1), filename));
				    }
				}));

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

		p.run();
	}
}
