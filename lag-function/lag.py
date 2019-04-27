import argparse
import logging

import apache_beam as beam
import apache_beam.transforms.window as window
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions

WINDOW_SECONDS = 10

class DuplicateWithLagDoFn(beam.DoFn):

  def process(self, element, timestamp=beam.DoFn.TimestampParam):
    logging.info("New message: {}".format(element))
    # Main output gets unmodified element
    yield element
    # The same element is emitted to the side output with a 1-window lag added to timestamp
    yield beam.pvalue.TaggedOutput('lag_output', beam.window.TimestampedValue(element, timestamp + WINDOW_SECONDS))


class CompareDoFn(beam.DoFn):
  # here we can access to previous value while processing the current one
  def process(self, element):
    logging.info("Combined with previous vale: {}".format(element))
 
    try:
      old_value = int(element[1][1][0].split(',')[1])
    except:
      old_value = 0

    try:
      new_value = int(element[1][0][0].split(',')[1])
    except:
      new_value = 0

    logging.info("New value: {}, Old value: {}, Difference: {}".format(new_value, old_value, new_value - old_value))
    return (element[0], new_value - old_value)

def run(argv=None):
  """Build and run the pipeline."""
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--output', required=True,
      help=('Output file to write to'))
  parser.add_argument(
      '--input', required=True,
      help=('Input PubSub topic of the form '
            '"projects/<PROJECT>/topics/<TOPIC>".'))
  known_args, pipeline_args = parser.parse_known_args(argv)

  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = True
  pipeline_options.view_as(StandardOptions).streaming = True
  p = beam.Pipeline(options=pipeline_options)

  results = (p 
    | 'Read messages' >> beam.io.ReadStringsFromPubSub(topic=known_args.input)
    | 'Assign keys' >> beam.Map(lambda a:(a.split(',')[0],a))
    | 'Add timestamps' >> beam.ParDo(DuplicateWithLagDoFn()).with_outputs('lag_output', main='main_output'))
 
  windowed_main = results.main_output | 'Window main output' >> beam.WindowInto(window.FixedWindows(WINDOW_SECONDS))
  windowed_lag = results.lag_output | 'Window lag output' >> beam.WindowInto(window.FixedWindows(WINDOW_SECONDS))

  merged = (windowed_main, windowed_lag) | 'Join Pcollections' >> beam.CoGroupByKey()

  merged | 'Compare' >> beam.ParDo(CompareDoFn())

  result = p.run()
  result.wait_until_finish()


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()

