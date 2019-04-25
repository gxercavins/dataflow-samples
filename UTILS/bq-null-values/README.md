## Write NULL values to BigQuery

Quick snippet to showcase how to write NULL values for certain BigQuery fields. This example originated from a [StackOverflow Question][1] and pretends only to showcase the workaround to avoid the `NullPointerException`. Credits for the base code go to `nomadic_squirrel` user.

## Example

In order to avoid the aforementioned exception being raised there is a simple workaround. We can pass the table schema to `BigQueryIO.writeTableRows()` and simply omit setting the value of the field when processing the elements in the `ParDo`.

We define the table schema, where `org_id` is `NULLABLE`:

``` java
List<TableFieldSchema> fields = new ArrayList<>();
fields.add(new TableFieldSchema().setName("ev_id").setType("STRING"));
fields.add(new TableFieldSchema().setName("customer_id").setType("STRING"));
fields.add(new TableFieldSchema().setName("org_id").setType("STRING").setMode("NULLABLE"));
TableSchema schema = new TableSchema().setFields(fields);
```

We just do not set any value for that field. In the code the line must be left commented out for this to work:

``` java
row.set( "ev_id",       "2323423423" );
row.set( "customer_id", "111111"     );
// row.set( "org_id",     Null         ); // Without this line, to avoid NPE
c.output( row );  
```

And finally we provide the table schema in the write step:

``` java
.apply( BigQueryIO.writeTableRows()
   .to( DATA_TABLE_OUT )
   .withSchema(schema)
   .withCreateDisposition( CREATE_NEVER )
   .withWriteDisposition( WRITE_APPEND ) );
```

A `NULL` value will be written to BigQuery as shown in the below image:

[![output table][2]][2]

This snippet was tested with the Java 2.5.0 SDK.

  [1]: https://stackoverflow.com/questions/51562802/nullpointerexception-when-outputting-tablerow-with-null-value/
  [2]: https://i.stack.imgur.com/cmbba.png

## License

These examples are provided under the Apache License 2.0.

## Issues

Report any issue to the GitHub issue tracker.
