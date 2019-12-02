import logging
import apache_beam as beam

PROJECT = "PROJECT_ID"
BUCKET = "BUCKET_NAME"
schema = "index:INTEGER,event:STRING"
FIELD_NAMES = ["index","event"]


class CsvToDictFn(beam.DoFn):
    def process(self, element):
        return [dict(zip(FIELD_NAMES, element.split(",")))]


def run():
    argv = [
        "--project={0}".format(PROJECT),
        "--runner=DirectRunner"
    ]

    p = beam.Pipeline(argv=argv)

    data = ['{0},good_line_{1}'.format(i + 1, i + 1) for i in range(10)]
    data.append('this is a bad row')

    events = (p
        | "Create data" >> beam.Create(data)
        | "CSV to dict" >> beam.ParDo(CsvToDictFn())
        | "Write results" >> beam.io.gcp.bigquery.WriteToBigQuery(
            "{0}:dataflow_test.good_lines".format(PROJECT),
            schema=schema,
        )
    )

    (events[beam.io.gcp.bigquery.BigQueryWriteFn.FAILED_ROWS]
        | "Bad lines" >> beam.io.textio.WriteToText("error_log.txt"))

    p.run()


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.DEBUG)
    run()
