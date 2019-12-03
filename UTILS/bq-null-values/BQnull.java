package com.dataflow.samples;

import java.util.ArrayList;
import java.util.List;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;

import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;


public class BQnull {

	public static void main(String[] args) {

		DataflowPipelineOptions options = PipelineOptionsFactory.fromArgs(args)
						.as(DataflowPipelineOptions.class); 
						
		Pipeline p = Pipeline.create(options);

		List<TableFieldSchema> fields = new ArrayList<>();
		fields.add(new TableFieldSchema().setName("ev_id").setType("STRING"));
		fields.add(new TableFieldSchema().setName("customer_id").setType("STRING"));
		fields.add(new TableFieldSchema().setName("org_id").setType("STRING").setMode("NULLABLE"));
		TableSchema schema = new TableSchema().setFields(fields);
		
		p
			.apply( "kicker", Create.of( "Kick!" ) )
			.apply( "Read values", ParDo.of( new DoFn<String, TableRow>() {
					@ProcessElement
					public void procesElement( ProcessContext c ) {
						TableRow row = new TableRow();
	
						row.set( "ev_id",       "2323423423" );
						row.set( "customer_id", "111111"     );
						// row.set( "org_id",     Null        ); // Without this line, no NPE
						c.output( row );  	
					}}))
			.apply( BigQueryIO.writeTableRows()
				.to("PROJECT_ID:DATASET.TABLE")
				.withSchema(schema)
				.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
				.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));
	
		p.run();
	}
}
