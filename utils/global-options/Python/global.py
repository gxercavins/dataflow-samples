import argparse, logging

import apache_beam as beam
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

class CustomPipelineOptions(PipelineOptions):

    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument(
            '--path',
            type=str,
            help='csv storage path')
        parser.add_value_provider_argument(
            '--table_name',
            type=str,
            help='Table Id')

def rewrite_values(element):
        """ Access options from within FlatMap """
        try:
            logging.info("File Path with str(): {}".format(str(custom_options.path.get())))
            logging.info("----------------------------")
            logging.info("element: {}".format(element))
            project_id = str(cloud_options.project)
            file_path = custom_options.path.get()
            table_name = custom_options.table_name.get()

            logging.info("project: {}".format(project_id))
            logging.info("File path: {}".format(file_path))
            logging.info("language: {}".format(table_name))
            logging.info("----------------------------")
        except Exception as e:
            logging.info("Error format----------------------------")
            raise KeyError(e)

        return file_path


def run(argv=None):
  parser = argparse.ArgumentParser()
  known_args, pipeline_args = parser.parse_known_args(argv)

  # declare options to be accessed globally
  global cloud_options
  global custom_options

  pipeline_options = PipelineOptions(pipeline_args)
  cloud_options = pipeline_options.view_as(GoogleCloudOptions)
  custom_options = pipeline_options.view_as(CustomPipelineOptions)
  pipeline_options.view_as(SetupOptions).save_main_session = True

  # Beginning of the pipeline
  p = beam.Pipeline(options=pipeline_options)

  init_data = (p
               | beam.Create(["Start"])
               | beam.FlatMap(rewrite_values))

  result = p.run()
  # result.wait_until_finish

if __name__ == '__main__':
  run()
