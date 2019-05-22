import argparse, logging, time

import apache_beam as beam
import apache_beam.transforms.window as window
from apache_beam.io import filesystems
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


class AddWindowingInfoFn(beam.DoFn):
  """output tuple of window(key) + element(value)"""
  def process(self, element, window=beam.DoFn.WindowParam):
    yield (window, element)


class WindowedWritesFn(beam.DoFn):
    """write one file per window/key"""
    def __init__(self, outdir):
        self.outdir = outdir

    def process(self, element):
        (window, elements) = element
        window_start = str(window.start.to_utc_datetime()).replace(" ", "_")
        window_end = str(window.end.to_utc_datetime()).replace(" ", "_")
        writer = filesystems.FileSystems.create(self.outdir + window_start + ',' + window_end + '.txt')

        for row in elements:
          writer.write(str(row) + "\n")

        writer.close()


def run(argv=None):
  parser = argparse.ArgumentParser()
  known_args, pipeline_args = parser.parse_known_args(argv)

  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = True
  p = beam.Pipeline(options=pipeline_options)

  data = [{'event': '{}'.format(event), 'timestamp': time.time() + 4*event} for event in range(10)]

  events = (p
    | 'Create Events' >> beam.Create(data) \
    | 'Add Timestamps' >> beam.Map(lambda x: beam.window.TimestampedValue(x, x['timestamp'])) \
    | 'Add Windows' >> beam.WindowInto(window.FixedWindows(10)) \
    | 'Add Window Info' >> beam.ParDo(AddWindowingInfoFn()) \
    | 'Group By Window' >> beam.GroupByKey() \
    | 'Windowed Writes' >> beam.ParDo(WindowedWritesFn('output/')))

  result = p.run()
  result.wait_until_finish()

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()