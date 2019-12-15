package com.dataflow.samples;

import java.io.IOException;

import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileIO.ReadMatches;
import org.apache.beam.sdk.io.FileIO.Match;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class CorruptZip {

	private static final Logger Log = LoggerFactory.getLogger(CorruptZip.class);

	public static interface MyOptions extends PipelineOptions {
		@Description("Input bucket (no gs:// prefix)")
		String getBucket();		
		void setBucket(String s);
	}

	@SuppressWarnings("serial")
	public static void main(String[] args) {
		MyOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(MyOptions.class);
		Pipeline p = Pipeline.create(options);

		String Bucket = options.getBucket();

		p
			.apply("Get file names",Create.of("gs://" + Bucket + "/corrupted-file-test/*")) 
			.apply(FileIO.matchAll())
			.apply(FileIO.readMatches())
			.apply(MapElements.via(new SimpleFunction <ReadableFile, String>() {
				public String apply(ReadableFile f) {
					String fileName = f.getMetadata().resourceId().toString();
					String temp = "";
					try{
						temp = f.readFullyAsUTF8String();
						Log.info(String.format("Successfully read file: %s", fileName));
					} catch(IOException e){
						Log.error(String.format("ERROR when reading file: %s", fileName));
					}

					return temp;
				}
			}));

		p.run();
	}
}
