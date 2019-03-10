import argparse
import logging
import re

import apache_beam as beam
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

# input file pattern will be a template parameter
class CustomPipelineOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument('--input',
                  dest='input',
                  default='gs://dataflow-samples/shakespeare/*.txt',
                  help='Input path with file to process.')
                  
def run(argv=None):
  """Main entry point; defines and runs the wordcount pipeline."""
  parser = argparse.ArgumentParser()
  known_args, pipeline_args = parser.parse_known_args(argv)

  pipeline_options = PipelineOptions(pipeline_args)
  custom_options = pipeline_options.view_as(CustomPipelineOptions)
  pipeline_options.view_as(SetupOptions).save_main_session = True
  p = beam.Pipeline(options=pipeline_options)

  p
    | 'Create' >> beam.Create(['Start!']) # just to kickstart the pipeline
    | 'Read Input Parameter' >> beam.Map(lambda x: custom_options.input.get()) # Map will accept the template parameter
    | 'Read All Files' >> beam.io.ReadAllFromText()
    | 'Write Results' >> WriteToText("gs://BUCKET-NAME/path/to/output.txt")

  result = p.run()
  result.wait_until_finish()
  
if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
