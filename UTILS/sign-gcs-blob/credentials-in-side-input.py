import argparse, json, logging
import datetime

import apache_beam as beam
import apache_beam.pvalue as pvalue
from apache_beam.io import ReadFromText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


class SignFileFn(beam.DoFn):
  """Signs GCS file with GCS-stored credentials"""
  def process(self, gcs_blob_path, creds):
    from google.cloud import storage
    from google.oauth2 import service_account

    credentials_json=json.loads('\n'.join(creds))
    credentials = service_account.Credentials.from_service_account_info(credentials_json)

    gcs_client = storage.Client(credentials=credentials)

    bucket = gcs_client.get_bucket(gcs_blob_path.split('/')[2])
    blob = bucket.blob('/'.join(gcs_blob_path.split('/')[3:]))

    url = blob.generate_signed_url(datetime.timedelta(seconds=300), method='GET')
    logging.info(url)
    yield url


def run(argv=None):
  parser = argparse.ArgumentParser()
  parser.add_argument('--key_file',
                    dest='key_file',
                    required=True,
                    help='Path to service account credentials JSON.')
  parser.add_argument('--input',
                    dest='input',
                    required=True,
                    help='GCS input file to sign.')
  known_args, pipeline_args = parser.parse_known_args(argv)

  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = True
  p = beam.Pipeline(options=pipeline_options)

  credentials = (p 
    | 'Read Credentials from GCS' >> ReadFromText(known_args.key_file))

  (p
    | 'Read File from GCS' >> beam.Create([known_args.input]) \
    | 'Sign File' >> beam.ParDo(SignFileFn(), pvalue.AsList(credentials)))

  result = p.run()
  result.wait_until_finish()


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
  