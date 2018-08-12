"""Normalize PCollection values."""

import logging
import argparse
import sys

import apache_beam as beam
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions


# custom CombineFn that outputs min and max value
class MinMaxFn(beam.CombineFn):
  # initialize min and max values (I assumed int type)
  def create_accumulator(self):
    return (sys.maxint, 0)

  # update if current value is a new min or max      
  def add_input(self, min_max, input):
    (current_min, current_max) = min_max
    return min(current_min, input), max(current_max, input)

  def merge_accumulators(self, accumulators):
    return accumulators

  def extract_output(self, min_max):
    return min_max


def run(argv=None):
  """Main entry point; defines and runs the pipeline."""
  parser = argparse.ArgumentParser()
  parser.add_argument('--output',
                      dest='output',
                      required=True,
                      help='Output file to write results to.')
  known_args, pipeline_args = parser.parse_known_args(argv)

  pipeline_options = PipelineOptions(pipeline_args)
  p = beam.Pipeline(options=pipeline_options)

  # create test data
  pc = p | 'Create' >> beam.Create([('foo', 1), ('bar', 5), ('foo', 5), ('bar', 9), ('bar', 2)])

  # first run through data to apply custom combineFn and determine min/max per key
  minmax = pc | 'Determine Min Max' >> beam.CombinePerKey(MinMaxFn())
  
  # group input data by key and append corresponding min and max 
  merged = (pc, minmax) | 'Join Pcollections' >> beam.CoGroupByKey()
 
  # apply mapping to normalize values according to 'norm_value = (value - min) / (max - min)'
  normalized = merged | 'Normalize values' >> beam.Map(lambda (a, (b, c)): (a, [float(val - c[0][0][0])/(c[0][0][1] -c[0][0][0]) for val in b]))

  # write results to output file
  normalized | 'Write results' >> WriteToText(known_args.output)

  result = p.run()
  # result.wait_until_finish()

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
