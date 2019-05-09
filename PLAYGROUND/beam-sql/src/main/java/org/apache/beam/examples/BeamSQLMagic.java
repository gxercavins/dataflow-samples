package org.apache.beam.examples;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

public class BeamSQLMagic {
    private static final Logger LOG = LoggerFactory.getLogger(BeamSQLMagic.class);

    public static final String HEADER = "year,month,day,wikimedia_project,language,title,views";
    public static final Schema SCHEMA = Schema.builder()
            .addStringField("lang")
            .addInt32Field("views")
            .build();

    public static void main(String[] args) {
        PipelineOptionsFactory.register(PipelineOptions.class);
        PipelineOptions options = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .as(PipelineOptions.class);
        Pipeline pipeline = Pipeline.create(options);

        final List<String> LINES = Arrays.asList(
          HEADER,
          "2006,April,01,tensorflow,python,tensorflow repo,3200",
          "2012,March,03,beam,java,dataflow repo,2000");

        pipeline.apply("create_input", Create.of(LINES))
                .apply("transform_to_row", ParDo.of(new RowParDo())).setRowSchema(SCHEMA)
                .apply("transform_sql", SqlTransform.query(
                        "SELECT lang, SUM(views) as sum_views " +
                                "FROM PCOLLECTION GROUP BY lang")
                )
                .apply("transform_to_string", ParDo.of(new RowToString()))
                .apply("write_to_gcs", TextIO.write().to("output.txt").withoutSharding());
                
        pipeline.run();
    }

    //ParDo for String -> Row (SQL)
    public static class RowParDo extends DoFn<String, Row> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            if (!c.element().equalsIgnoreCase(HEADER)) {
                String[] vals = c.element().split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
                Row appRow = Row
                        .withSchema(SCHEMA)
                        .addValues(vals[4], Integer.valueOf(vals[6]))
                        .build();
                LOG.info(appRow.toString());
                c.output(appRow);
            }
        }
    }

    //ParDo for Row (SQL) -> String
    public static class RowToString extends DoFn<Row, String> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            String line = c.element().getValues()
                    .stream()
                    .map(Object::toString)
                    .collect(Collectors.joining(","));
            c.output(line);
        }
    }
}