package com.dataflow.samples;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead.Method;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.Contextful;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;


public abstract class BigQueryStorageAPI {

    public interface Options extends PipelineOptions {
        @Validation.Required
        @Description("Output Path i.e. gs://BUCKET/path/to/output/folder")
        String getOutput();
        void setOutput(String s);
    }

    public static void main(String[] args) {

        BigQueryStorageAPI.Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(BigQueryStorageAPI.Options.class);

        Pipeline p = Pipeline.create(options);

        String output = options.getOutput();

		PCollection<TableRow> inputRows = p
		    .apply(BigQueryIO.readTableRows()
		        .from("bigquery-public-data:london_bicycles.cycle_stations")
		        .withMethod(Method.DIRECT_READ)
		        .withSelectedFields(Lists.newArrayList("id", "name")));

		inputRows
			.apply(MapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.strings()))
		    	.via(tableRow -> KV.of((String) tableRow.get("id"),(String) tableRow.get("name"))))
		    .apply(FileIO.<String, KV<String, String>>writeDynamic()
		    	.by(KV::getKey)
		    	.withDestinationCoder(StringUtf8Coder.of())
		    	.via(Contextful.fn(KV::getValue), TextIO.sink())
		    	.to(output)
		    	.withNaming(key -> FileIO.Write.defaultNaming("file-" + key, ".txt")));

        p.run().waitUntilFinish();
    }
}
