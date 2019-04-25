import argparse, logging, time
from random import randint
import inflect

import apache_beam as beam
from apache_beam.transforms.userstate import BagStateSpec
import apache_beam.transforms.window as window
from apache_beam.coders import IterableCoder
from apache_beam.coders import StrUtf8Coder
from apache_beam.coders import PickleCoder
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


class LogFn(beam.DoFn):
  """Logs pairs"""
  def process(self, element):
    logging.info(element)
    yield element


class PairRecordsFn(beam.DoFn):
  """Pairs two consecutive elements after shuffle"""
  BUFFER = BagStateSpec('buffer', PickleCoder())
  def process(self, element, buffer=beam.DoFn.StateParam(BUFFER)):
    try:
      previous_element = list(buffer.read())[0]
    except:
      previous_element = []
    unused_key, value = element

    if previous_element:
      yield (previous_element, value)
      buffer.clear()
    else:
      buffer.add(value)


def run(argv=None):
  parser = argparse.ArgumentParser()
  known_args, pipeline_args = parser.parse_known_args(argv)

  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = True
  p = beam.Pipeline(options=pipeline_options)

  q = inflect.engine()
  data = [str(q.number_to_words(i)) for i in range(32)]

  pairs = (p
    | 'Create Events' >> beam.Create(data)
    | 'Add Keys' >> beam.Map(lambda x: (randint(1,4), x))
    | 'Pair Records' >> beam.ParDo(PairRecordsFn())
    | 'Check Results' >> beam.ParDo(LogFn()))

  result = p.run()
  result.wait_until_finish()

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
