package com.dataflow.samples;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.Contextful;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;


public abstract class OneRowOneFile {

    public interface Options extends PipelineOptions {
        @Validation.Required
        @Description("Output Path i.e. gs://BUCKET/path/to/output/folder")
        String getOutput();
        void setOutput(String s);
    }

    public static void main(String[] args) {

        OneRowOneFile.Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(OneRowOneFile.Options.class);

        Pipeline p = Pipeline.create(options);

        String output = options.getOutput();

        p.apply("Create Data", Create.of(KV.of("one", "this is row 1"), KV.of("two", "this is row 2"), KV.of("three", "this is row 3"), KV.of("four", "this is row 4")))
         .apply(FileIO.<String, KV<String, String>>writeDynamic()
            .by(KV::getKey)
            .withDestinationCoder(StringUtf8Coder.of())
            .via(Contextful.fn(KV::getValue), TextIO.sink())
            .to(output)
            .withNaming(key -> FileIO.Write.defaultNaming("file-" + key, ".txt")));

        p.run().waitUntilFinish();
    }
}
