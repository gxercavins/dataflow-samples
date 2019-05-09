## Beam SQL

[Beam SQL](https://beam.apache.org/documentation/dsls/sql/overview/) allows you to run SQL queries on bounded and unbounded PCollections. For a very good introduction I recommend reading [this blog article](https://medium.com/weareservian/exploring-beam-sql-on-google-cloud-platform-b6c77f9b4af4). I decided to play around with the example there as well as reproducing the use case in a [StackOverflow question](https://stackoverflow.com/questions/55851708/beamsql-output-file-is-empty#comment98511712_55851708). The underlying code is provided by the corresponding authors, I just added some in-memory input data but I thought it would be good to provide the full example with imports, pom file, etc.

Note that the following extension needs to be added to `pom.xml`:

```xml
<!-- https://mvnrepository.com/artifact/org.apache.beam/beam-sdks-java-extensions-sql -->
<dependency>
    <groupId>org.apache.beam</groupId>
    <artifactId>beam-sdks-java-extensions-sql</artifactId>
    <version>${beam.version}</version>
</dependency>
```
## Example

For the `BeamSQLMagic.java` file all explanations can be found [here](https://medium.com/weareservian/exploring-beam-sql-on-google-cloud-platform-b6c77f9b4af4). 

In `BeamSQL.java`, we expect our query to return all available fields, so we'll use the following schema:

```java
public static final Schema appSchema = Schema.builder()
        .addStringField("artist_credit")
        .addStringField("position")
        .addStringField("artist")
        .addStringField("name")
        .addStringField("join_phrase") 
        .build();
```

so `RowParDo` will split each line into its comma-separated values and build the Beam SQL `Row`:

```java
public static class RowParDo extends DoFn<String, Row> {
    @ProcessElement
    public void processElement(ProcessContext c) {
        if (!c.element().equalsIgnoreCase(HEADER)) {
            String[] vals = c.element().split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
            Row appRow = Row
                    .withSchema(appSchema)
                    .addValues(vals[0], vals[1], vals[2], vals[3], vals[4])
                    .build();
            c.output(appRow);
        }
    }
}
```

Our query will filter and return only rows where `position` is equal to 1:

```java
.apply("transform_sql", SqlTransform.query(
        "SELECT artist_credit, `position`, artist, name, join_phrase " +
                "FROM PCOLLECTION WHERE `position` = '1'")
)
```

If we use the following dummy in-memory data as input:

```java
final List<String> LINES = Arrays.asList(
  HEADER,
  "John Lennon,1,The Beatles,A Day in the Life,and",
  "Paul McCartney,2,The Beatles,A Day in the Life,.",
  "Jimmy Page,1,Led Zeppelin,Stairway to Heaven,and",
  "Robert Plant,2,Led Zeppelin,Stairway to Heaven,.");
```

once we filter with `position = ' 1'` the output file will contain:

```bash
John Lennon,1,The Beatles,A Day in the Life,and
Jimmy Page,1,Led Zeppelin,Stairway to Heaven,and
```

## License

These examples are provided under the Apache License 2.0.

## Issues

Report any issue to the GitHub issue tracker.
