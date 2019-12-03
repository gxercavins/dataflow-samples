package com.dataflow.samples;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.Method;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;


public class NonStandardDelimiters {

	public static interface MyOptions extends PipelineOptions {
		@Validation.Required
		@Description("Output BigQuery table <PROJECT_ID>:<DATASET>.<TABLE>")
		ValueProvider<String> getOutput();
		void setOutput(ValueProvider<String> s);

		@Validation.Required
		@Description("Input file, gs://path/to/file")
		ValueProvider<String> getInput();		
		void setInput(ValueProvider<String> s);

		@Description("Custom delimiter, default is comma (,)")
		@Default.String(",")
		ValueProvider<String> getDelimiter();		
		void setDelimiter(ValueProvider<String> s);
	}

	@SuppressWarnings("serial")
	public static void main(String[] args) {
		MyOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(MyOptions.class);

		Pipeline p = Pipeline.create(options);

		ValueProvider<String> file = options.getInput();
		ValueProvider<String> output = options.getOutput();
		ValueProvider<String> delimiter = options.getDelimiter();

		// Build the table schema for the output table.
		List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("data").setType("STRING"));
		TableSchema schema = new TableSchema().setFields(fields);

		p
			.apply("GetMessages", TextIO.read().from(file))
            .apply("ExtractRows", ParDo.of(new DoFn<String, String>() {
		        @ProcessElement
		        public void processElement(ProcessContext c) {
		          for (String line : c.element().split(delimiter.get().replace("\\", "\\\\"))) {
		            if (!line.isEmpty()) {
		              c.output(line);
		            }
		          }
		        }
		    }))
			.apply("ToBQRow", ParDo.of(new DoFn<String, TableRow>() {
				@ProcessElement
				public void processElement(ProcessContext c) throws Exception {
					TableRow row = new TableRow();
					row.set("data", c.element());
					c.output(row);
				}
			}))
			.apply(BigQueryIO.writeTableRows().to(output)
					.withSchema(schema)
					.withMethod(Method.FILE_LOADS)
					.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
					.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));

		p.run();
	}
}
