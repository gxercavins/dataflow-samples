from windowing.dynamic import DynamicSessions

import argparse, json, logging, time

import apache_beam as beam
import apache_beam.transforms.window as window
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


class AnalyzeSession(beam.DoFn):
  """Prints per session information"""
  def process(self, element, window=beam.DoFn.WindowParam):
    user = element[0]
    num_events = str(len(element[1]))
    window_end = window.end.to_utc_datetime()
    window_start = window.start.to_utc_datetime()
    session_duration = window_end - window_start

    total_score = 0

    for elem in element[1]:
      total_score += elem["score"]

    logging.info(">> User %s had %s events with total score %s in a %s session", user, num_events, total_score, session_duration)

    yield element


def run(argv=None):
  parser = argparse.ArgumentParser()
  known_args, pipeline_args = parser.parse_known_args(argv)

  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = True
  p = beam.Pipeline(options=pipeline_options)

  gap_size = 10 # [s]; 10 seconds

  initial_time = time.time()

  user1_data = [{'user': 'mobile', 'score': 12, 'timestamp': initial_time, 'gap': 5},
                {'user': 'mobile', 'score': -3, 'timestamp': initial_time + 2, 'gap': 5},
                {'user': 'mobile', 'score': 2, 'timestamp': initial_time + 9, 'gap': 5},
                {'user': 'mobile', 'score': 7, 'timestamp': initial_time + 12, 'gap': 5}]
  user2_data = [{'user': 'desktop', 'score': 4, 'timestamp': initial_time},
                {'user': 'desktop', 'score': 10, 'timestamp': initial_time + 12}]

  events = (p
    | 'Create Events' >> beam.Create(user1_data + user2_data) \
    | 'Add Timestamps' >> beam.Map(lambda x: beam.window.TimestampedValue(x, x['timestamp'])) \
    | 'keyed_on_user_id'      >> beam.Map(lambda x: (x['user'], x))
    | 'user_session_window'   >> beam.WindowInto(DynamicSessions(gap_size=gap_size),
                                               timestamp_combiner=window.TimestampCombiner.OUTPUT_AT_EOW) \
    | 'Group' >> beam.GroupByKey() \
    | 'analyze_session'         >> beam.ParDo(AnalyzeSession()))

  result = p.run()
  result.wait_until_finish()

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()