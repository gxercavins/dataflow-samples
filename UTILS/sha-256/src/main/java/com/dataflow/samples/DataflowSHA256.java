package com.dataflow.samples;

import java.io.IOException;
import java.security.MessageDigest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileIO.ReadMatches;
import org.apache.beam.sdk.io.FileIO.Match;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.TypeDescriptors;

public class DataflowSHA256 {

  private static final Logger Log = LoggerFactory.getLogger(DataflowSHA256.class);

  public static interface MyOptions extends PipelineOptions {
    @Validation.Required
    @Description("Input path (with gs:// prefix)")
    String getInput();   
    void setInput(String s);
  }

  @SuppressWarnings("serial")
  public static void main(String[] args) {
    MyOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(MyOptions.class);
    Pipeline p = Pipeline.create(options);

    p
      .apply("Match Filenames", FileIO.match().filepattern(options.getInput()))
      .apply("Read Matches", FileIO.readMatches())
      .apply(MapElements.via(new SimpleFunction <ReadableFile, KV<String,String>>() {
          public KV<String,String> apply(ReadableFile f) {
                String temp = null;
                try{
                    temp = f.readFullyAsUTF8String();
                }catch(IOException e){

                }

                String sha256hex = org.apache.commons.codec.digest.DigestUtils.sha256Hex(temp);   
                
                return KV.of(f.getMetadata().resourceId().toString(), sha256hex);
            }
          }
      ))
      .apply("Print results", ParDo.of(new DoFn<KV<String, String>, Void>() {
          @ProcessElement
          public void processElement(ProcessContext c) {
            Log.info(String.format("File: %s, SHA-256: %s ", c.element().getKey(), c.element().getValue()));
          }
        }
      ));

    p.run();
  }
}