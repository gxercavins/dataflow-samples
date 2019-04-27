import argparse, json, logging, time

import apache_beam as beam
import apache_beam.transforms.window as window
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


class AnalyzeSession(beam.DoFn):
  """Prints per session information"""
  def process(self, element, window=beam.DoFn.WindowParam):
    # uncomment below line to log everything like in description
    # logging.info(element)

    # alternatively, extract per-session statistics
    user = element[0]
    num_events = str(len(element[1]))
    window_end = window.end.to_utc_datetime()
    window_start = window.start.to_utc_datetime()
    session_duration = window_end - window_start

    logging.info(">>> User %s had %s events in %s session", user, num_events, session_duration)

    yield element


def run(argv=None):
  parser = argparse.ArgumentParser()
  known_args, pipeline_args = parser.parse_known_args(argv)

  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = True
  p = beam.Pipeline(options=pipeline_options)

  session_gap = 5 # [s]; 5 seconds
  user1_data = [{'user_id': 'Thanos', 'value': 'event_{}'.format(event), 'timestamp': time.time() + 2**event} for event in range(5)]
  user2_data = [{'user_id': 'Groot', 'value': 'event_{}'.format(event), 'timestamp': time.time() + 1 + 3**event} for event in range(3)]

  events = (p
    | 'Create Events' >> beam.Create(user1_data + user2_data) \
    | 'Add Timestamps' >> beam.Map(lambda x: beam.window.TimestampedValue(x, x['timestamp'])) \
    | 'keyed_on_user_id'      >> beam.Map(lambda x: (x['user_id'], x))
    | 'user_session_window'   >> beam.WindowInto(window.Sessions(session_gap),
                                               timestamp_combiner=window.TimestampCombiner.OUTPUT_AT_EOW) \
    | 'Group' >> beam.GroupByKey() \
    | 'analyze_session'         >> beam.ParDo(AnalyzeSession()))

  result = p.run()
  result.wait_until_finish()

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()