import argparse, json, io, base64, sys, uuid

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.io import filesystems
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

from PIL import Image, ImageDraw
import numpy as np


class EncodeFn(beam.DoFn):
    def process(self, element, img_siz=128, lw=3):
        img = Image.new('RGB', (img_size, img_size), (255,255,255))
        draw = ImageDraw.Draw(img)
        element['drawing'] = [np.array(stroke) for stroke in element['drawing']]
        lines = np.array([
            stroke[0:2, i:i+2]
            for stroke in element['drawing']
            for i in range(stroke.shape[1] - 1)
        ], dtype=np.float32)
        lines /= 1024
        for line in lines:
            draw.line(tuple(line.T.reshape((-1,)) * img_size), fill='black', width=lw)
        b = io.BytesIO()
        img.convert('RGB').save(b, format='png')
        yield {'key_id':element['key_id'], 'image':b.getvalue()}

class WriteToSeparateFiles(beam.DoFn):
    def __init__(self, outdir):
        self.outdir = outdir
    def process(self, element):
        writer = filesystems.FileSystems.create(self.outdir + element['key_id'] + '.png')
        writer.write(element['image'])
        writer.close()

def run(argv=None):
  parser = argparse.ArgumentParser()
  parser.add_argument('--output',
                      dest='output',
                      required=True,
                      help='GCS destination folder to save the images to (example: gs://BUCKET_NAME/path/to/images/')
  known_args, pipeline_args = parser.parse_known_args(argv)

  # Use save_main_session option because EncodeFn relies on global context (Image module imported)
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = True
  p = beam.Pipeline(options=pipeline_options)

  input = p | 'Read files' >> ReadFromText("gs://quickdraw_dataset/full/raw/owl.ndjson")
  jsons = input | 'Load into JSON' >> beam.Map(lambda x: json.loads(x))
  b64 = jsons | 'Convert to images' >> beam.ParDo(EncodeFn())
  b64 | 'Save images' >> beam.ParDo(WriteToSeparateFiles(known_args.output))

  result = p.run()
  # result.wait_until_finish()

if __name__ == '__main__':
  run()