import argparse, logging

import apache_beam as beam
import apache_beam.transforms.combiners as combine
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


class SaveMaxFn(beam.DoFn):
  """Stores max in global variable"""
  def process(self, element):
    length = element[0][1]
    logging.info("Longest sentence: %s token(s)", length)

    return element


def run(argv=None):
  parser = argparse.ArgumentParser()
  known_args, pipeline_args = parser.parse_known_args(argv)

  global length

  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = True
  p = beam.Pipeline(options=pipeline_options)

  sentences = sentences = ['This is the first sentence',
             'Second sentence',
             'Yet another sentence']

  longest_sentence = (p
    | 'Read Sentences' >> beam.Create(sentences)
    | 'Split into Words' >> beam.Map(lambda x: x.split(' '))
    | 'Map Token Length'      >> beam.Map(lambda x: (x, len(x)))
    | 'Top Sentence' >> combine.Top.Of(1, lambda a,b: a[1]<b[1])
    | 'Save Variable'         >> beam.ParDo(SaveMaxFn()))

  result = p.run()
  result.wait_until_finish()

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
