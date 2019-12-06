import argparse, json, logging, time

import apache_beam as beam
import apache_beam.transforms.window as window
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


class DebugPrinterFn(beam.DoFn):
  """Just prints the element and window"""
  def process(self, element, window=beam.DoFn.WindowParam):
    logging.info("Received message %s in window=%s", element['message'], window)
    yield element


def run(argv=None):
  parser = argparse.ArgumentParser()
  known_args, pipeline_args = parser.parse_known_args(argv)

  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = True
  p = beam.Pipeline(options=pipeline_options)

  data = [{'message': 'Hi', 'timestamp': time.time()}]

  events = (p
    | 'Create Events' >> beam.Create(data) \
    | 'Add Timestamps' >> beam.Map(lambda x: beam.window.TimestampedValue(x, x['timestamp'])) \
    | 'Sliding Windows'   >> beam.WindowInto(beam.window.SlidingWindows(60, 60)) \
    | 'First window' >> beam.ParDo(DebugPrinterFn()) \
    | 'global Window'   >> beam.WindowInto(window.GlobalWindows()) \
    | 'Second window'   >> beam.ParDo(DebugPrinterFn()))

  result = p.run()
  result.wait_until_finish()

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()