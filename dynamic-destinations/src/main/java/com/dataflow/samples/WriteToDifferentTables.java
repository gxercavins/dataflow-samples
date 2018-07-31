package com.dataflow.samples;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.DynamicDestinations;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.bigquery.model.TableFieldSchema;

import java.util.ArrayList;
import java.util.List;

/**
 * An example that writes each row entry to different BigQuery tables, each one with different number of columns. 
 * The first column indicates the number of features
 *
 * For large input files and high number of schemas it is recommended to select a larger machine type. As an example:
 * {@code --workerMachineType=n1-standard-8}
 */

public abstract class WriteToDifferentTables {

    public interface Options extends DataflowPipelineOptions {
        @Description("Output BigQuery Table Prefix <project_id>:<dataset>.<table_prefix>")
        String getOutput();

        void setOutput(String s);

        @Description("Path to Input CSV File (i.e. gs://BUCKET_NAME/input.csv")
        String getInput();
        
void setInput(String s);
    }

    public static void main(String[] args) {

        WriteToDifferentTables.Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(WriteToDifferentTables.Options.class);

        Pipeline p = Pipeline.create(options);

        String input = options.getInput();
        String output = options.getOutput();

        p.apply("ReadLines", TextIO.read().from(input))
        .apply(BigQueryIO.<String>write()
        .to(new DynamicDestinations<String, String>() {
        	public String getDestination(ValueInSingleWindow<String> element) {
          		return element.getValue().split(",")[0];
        	}
        	public TableDestination getTable(String num) {
                        String tableName = num.replace("\"", "");
                        String tableSpec = output + tableName;
          		return new TableDestination(tableSpec, "Table for type " + tableName);
	        }
        	public TableSchema getSchema(String schema) {
        		List<TableFieldSchema> fields = new ArrayList<>();
			int iterations = Integer.parseInt(schema.replace("\"", ""));
                        for(int i=1; i<=iterations; i++){
	                        fields.add(new TableFieldSchema().setName("Field" + Integer.toString(i)).setType("STRING"));
                        }
                        TableSchema ts = new TableSchema();
                        ts.setFields(fields);
                        return ts;
        	}
      	})
	.withFormatFunction(new SerializableFunction<String, TableRow>() {
     		public TableRow apply(String row) {
			TableRow tr = new TableRow();
			int iterations2 = Integer.parseInt(row.split(",")[0].replace("\"", ""));
                        for(int i=1; i<=iterations2; i++){
	                        tr.set("Field" + Integer.toString(i), row.split(",")[i].replace("\"", ""));
                        }
                        return tr;
     		}
   	}));

        p.run().waitUntilFinish();

    }

}
