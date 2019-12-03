import argparse, json, logging

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


class ExtractFn(beam.DoFn):
    def process(self, element):
    	file_name = 'gs://' + "/".join(element['id'].split("/")[:-1])
    	logging.info('File: ' + file_name) 
        yield file_name

class LogFn(beam.DoFn):
    def process(self, element):
        logging.info(element)
        yield element


def run(argv=None):
  parser = argparse.ArgumentParser()
  known_args, pipeline_args = parser.parse_known_args(argv)

  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = True
  p = beam.Pipeline(options=pipeline_options)

  (p
    | 'Read Messages' >> beam.io.ReadFromPubSub(topic="projects/PROJECT/topics/TOPIC")
    | 'Convert Message to JSON' >> beam.Map(lambda message: json.loads(message))
    | 'Extract File Names' >> beam.ParDo(ExtractFn())
    | 'Read Files' >> beam.io.ReadAllFromText()
    | 'Write Results' >> beam.ParDo(LogFn()))

  result = p.run()
  result.wait_until_finish()

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
