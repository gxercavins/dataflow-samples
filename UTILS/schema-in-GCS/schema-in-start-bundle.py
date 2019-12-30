import argparse, json, logging

import apache_beam as beam
import apache_beam.pvalue as pvalue
from apache_beam.io import ReadFromText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


BUCKET='BUCKET_NAME'


class EnrichElementsFn(beam.DoFn):
  """Zips data with schema stored in GCS"""
  def start_bundle(self):
    from google.cloud import storage

    client = storage.Client()
    blob = client.get_bucket(BUCKET).get_blob('schema.json')
    self.schema = blob.download_as_string()

  def process(self, element):
    field_names = [x['name'] for x in json.loads(self.schema)]
    yield zip(field_names, element)


class LogElementsFn(beam.DoFn):
  """Prints element information"""
  def process(self, element):
    logging.info(element)
    yield element


def run(argv=None):
  parser = argparse.ArgumentParser()
  known_args, pipeline_args = parser.parse_known_args(argv)

  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = True
  p = beam.Pipeline(options=pipeline_options)

  data = [('NC', 'F', 2020, 'Hello', 3200),
          ('NC', 'F', 2020, 'World', 3180)]

  (p
    | 'Create Events' >> beam.Create(data) \
    | 'Enrich with start bundle' >> beam.ParDo(EnrichElementsFn()) \
    | 'Log elements' >> beam.ParDo(LogElementsFn()))

  result = p.run()
  result.wait_until_finish()


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
  
