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

public class BeamSQL {
    private static final Logger LOG = LoggerFactory.getLogger(BeamSQL.class);

    public static final String HEADER = "artist_credit,position,artist,name,join_phrase";
    public static final Schema appSchema = Schema.builder()
            .addStringField("artist_credit")
            .addStringField("position")
            .addStringField("artist")
            .addStringField("name")
            .addStringField("join_phrase") 
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
          "John Lennon,1,The Beatles,A Day in the Life,and",
          "Paul McCartney,2,The Beatles,A Day in the Life,.",
          "Jimmy Page,1,Led Zeppelin,Stairway to Heaven,and",
          "Robert Plant,2,Led Zeppelin,Stairway to Heaven,.");

        pipeline.apply("create_input", Create.of(LINES))
                .apply("transform_to_row", ParDo.of(new RowParDo())).setRowSchema(appSchema)
                .apply("transform_sql", SqlTransform.query(
                        "SELECT artist_credit, `position`, artist, name, join_phrase " +
                                "FROM PCOLLECTION WHERE `position` = '1'")
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
                        .withSchema(appSchema)
                        .addValues(vals[0], vals[1], vals[2], vals[3], vals[4])
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