package com.dataflow.samples;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileIO.ReadMatches;
import org.apache.beam.sdk.io.FileIO.Match;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.TextIO.Write;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Keys;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;

public class RegexFileIO {

	private static final Logger Log = LoggerFactory.getLogger(RegexFileIO.class);

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

        // create a couple dummy file pattern and source pairs (this could be passed as an option instead)
		PCollection<KV<String, String>> filesAndSources = p.apply("Create file pattern and source pairs",
			Create.of(KV.of("gs://" + Bucket + "/sales/*", "Sales"), KV.of("gs://" + Bucket + "/events/*", "Events")));

 		 // Create a map PCollectionView
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
		  .apply("Process according to type", ParDo.of(new DoFn<KV<String, String>, Void>() {
			@ProcessElement
			public void processElement(ProcessContext c) {
				Log.info(String.format("key=%s, value=%s", c.element().getKey(), c.element().getValue()));
			}
		}));

		p.run();
	}
}
