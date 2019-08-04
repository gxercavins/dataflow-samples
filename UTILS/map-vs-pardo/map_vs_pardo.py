import argparse, logging

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


def compute_interest_map(data_item):
  return data_item + 1

class compute_interest_pardo(beam.DoFn):
  def process(self, element):
    yield element + 2

class log_results(beam.DoFn):
  def process(self, element):
    logging.info(">> Interest: %s", element)


def run(argv=None):
  parser = argparse.ArgumentParser()
  known_args, pipeline_args = parser.parse_known_args(argv)

  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = True
  p = beam.Pipeline(options=pipeline_options)

  events = (p
    | 'Create' >> beam.Create([1, 2, 3]) \
    | 'Add 1' >> beam.Map(lambda x: compute_interest_map(x)) \
    | 'Add 2' >> beam.ParDo(compute_interest_pardo()) \
    | 'Print' >> beam.ParDo(log_results()))

  result = p.run()
  result.wait_until_finish()

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()