#mfrom combiners.top import TopDistinctFn

import argparse, json, logging, time
from random import choice, randint

import apache_beam as beam
import apache_beam.transforms.window as window
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
import apache_beam.transforms.combiners as combine

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

  inputs = (p
    | 'Read Messages' >> beam.io.ReadFromPubSub(topic='projects/PROJECT/topics/TOPIC'))

  (inputs
         | 'Add User as key' >> beam.Map(lambda x: (x, 1)) # ('key', 1)
         | 'Apply Window of time' >> beam.WindowInto(
                        beam.window.FixedWindows(size=10*60),
                        trigger=beam.trigger.Repeatedly(beam.trigger.AfterCount(2)),
                        accumulation_mode=beam.trigger.AccumulationMode.ACCUMULATING)
         | 'Sum Score' >> beam.CombinePerKey(sum)   
         #| 'Latest User Score' >> beam.CombinePerKey(
         #               beam.combiners.TopCombineFn(n=1, compare=lambda a, b: a < b))   
         # | beam.combiners.Top.LargestPerKey(1) 
         | 'Top 10 scores' >> beam.CombineGlobally(
                        beam.combiners.TopCombineFn(n=10, compare=lambda a, b: a[1] < b[1])).without_defaults()
         | 'Print results' >> beam.ParDo(PrintTop10Fn()))

  result = p.run()
  result.wait_until_finish()

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()

