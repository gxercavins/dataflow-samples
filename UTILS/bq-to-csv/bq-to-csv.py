import argparse

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

query = 'SELECT word, word_count, corpus FROM `bigquery-public-data.samples.shakespeare` WHERE CHAR_LENGTH(word) > 3 ORDER BY word_count DESC LIMIT 10'

def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--bucket',
        dest='BUCKET',
        required=True,
        help='GCS Bucket name for output CSV (no gs:// prefix).')
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    p = beam.Pipeline(options=pipeline_options)

    # Execute the SQL query from public dataset
    BQ_DATA = p | 'read_bq_view' >> beam.io.Read(
        beam.io.BigQuerySource(query=query, use_standard_sql=True))

    # Convert to CSV format
    BQ_VALUES = BQ_DATA | 'read values' >> beam.Map(lambda x: x.values())
    BQ_CSV = BQ_VALUES | 'CSV format' >> beam.Map(lambda row: ', '.join(['"'+ str(column) +'"' for column in row]))

    # Write results to GCS with .csv suffix
    BQ_CSV | 'Write_to_GCS' >> beam.io.WriteToText('gs://{0}/results/output'.format(known_args.BUCKET), file_name_suffix='.csv', header='word, word count, corpus')
    
    result = p.run()
    result.wait_until_finish()

if __name__ == '__main__':
   run()
