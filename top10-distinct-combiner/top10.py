import argparse, json, logging, time
from random import choice, randint

import apache_beam as beam
import apache_beam.transforms.window as window
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


class PrintTop10Fn(beam.DoFn):
  """Prints Top 10 user score by num of events"""
  def process(self, element):
    logging.info(">>> Current top 10: %s", element)
    yield element


def run(argv=None):
  parser = argparse.ArgumentParser()
  known_args, pipeline_args = parser.parse_known_args(argv)

  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = True
  p = beam.Pipeline(options=pipeline_options)

  name_list = ['Paul', 'Michel', 'Hugo', 'Bob', 'Kevin', 'Bacon', 'Laura', 'Anne', 'John', 'Connor', 'Diane', 'Louis', 'Eva', 'Charles', 'Marta']
  data = [{'name': '{}'.format(choice(name_list)), 'timestamp': time.time() + randint(1,60)} for i in range(1000)]

  inputs = (p
    | 'Create Events' >> beam.Create(data)
    | 'Add Timestamps' >> beam.Map(lambda x: beam.window.TimestampedValue(x, x['timestamp'])))

  (inputs
         | 'Add User as key' >> beam.Map(lambda x: (x['name'], 1)) # ('key', 1)
         | 'Apply Window of time' >> beam.WindowInto(
                        beam.window.FixedWindows(size=10*60),
                        trigger=beam.trigger.AfterWatermark(early=beam.trigger.Repeatedly(beam.trigger.AfterCount(5))),
                        accumulation_mode=beam.trigger.AccumulationMode.DISCARDING)
         | 'Sum Score' >> beam.CombinePerKey(sum)         
         | 'Top 10 scores' >> beam.CombineGlobally(
                        beam.combiners.TopCombineFn(n=10, compare=lambda a, b: a[1] < b[1])).without_defaults()
         | 'Print results' >> beam.ParDo(PrintTop10Fn()))

  result = p.run()
  result.wait_until_finish()

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()

