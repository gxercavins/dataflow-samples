import argparse, logging

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


def get_table_name(element):
  """
  The function will return a fully qualified table name according to the logic applied to the processed element.
  In this case, input data already provides the table name directly but it can be extended to more complex use cases.
  """
  return 'PROJECT_ID:DATASET.' + element['type']


def run(argv=None):
  parser = argparse.ArgumentParser()
  known_args, pipeline_args = parser.parse_known_args(argv)

  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = True
  p = beam.Pipeline(options=pipeline_options)

  data = [{'name': 'A brave new world', 'author': 'Aldous Huxley', 'type': 'book'},
          {'name': 'Blade runner', 'director': 'Ridley Scott', 'type': 'movie'},
          {'name': 'The wall', 'band': 'Pink Floyd', 'type': 'album'},
          {'name': '1984', 'author': 'George Orwell', 'type': 'book'}]

  (p
    | 'Create Events' >> beam.Create(data) \
    | 'Dynamic Writes' >> beam.io.gcp.bigquery.WriteToBigQuery(table=get_table_name))

  result = p.run()
  result.wait_until_finish()

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()