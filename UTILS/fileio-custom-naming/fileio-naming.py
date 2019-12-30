import argparse, json, logging

import apache_beam as beam
from apache_beam.io import fileio
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


class JsonSink(fileio.TextSink):
  def write(self, record):
    self._fh.write(json.dumps(record).encode('utf8'))
    self._fh.write('\n'.encode('utf8'))


class PrintHashFn(beam.DoFn):
  """Print element and hash"""
  def process(self, element):
    logging.info("Element: %s with hash %s", element, hash(element))
    yield element


def hash_naming(*args):
  file_name = fileio.destination_prefix_naming()(*args)  # -1885604661473532601----00000-00001
  destination = file_name.split('----')[0]  # -1885604661473532601
  return '{}.json'.format(destination)  # -1885604661473532601.json


def run(argv=None):
  parser = argparse.ArgumentParser()
  known_args, pipeline_args = parser.parse_known_args(argv)

  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = True
  p = beam.Pipeline(options=pipeline_options)

  data = [{'id': 0, 'message': 'hello'},
          {'id': 1, 'message': 'world'}]

  (p
    | 'Create Events' >> beam.Create(data) \
    | 'JSONify' >> beam.Map(json.dumps) \
    | 'Print Hashes' >> beam.ParDo(PrintHashFn()) \
    | 'Write Files' >> fileio.WriteToFiles(
        path='./output',
        destination=lambda record: hash(record),
        sink=lambda dest: JsonSink(),
        file_naming=hash_naming))

  result = p.run()
  result.wait_until_finish()

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()