#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""Dynamic Sessions.

Custom windowing function classes can be created, by subclassing from
WindowFn.
"""

from __future__ import absolute_import

import abc
from builtins import object
from builtins import range
from functools import total_ordering

from future.utils import with_metaclass
from google.protobuf import duration_pb2
from google.protobuf import timestamp_pb2

from apache_beam.coders import coders
from apache_beam.portability import common_urns
from apache_beam.portability import python_urns
from apache_beam.portability.api import beam_runner_api_pb2
from apache_beam.portability.api import standard_window_fns_pb2
from apache_beam.transforms import timeutil
from apache_beam.utils import proto_utils
from apache_beam.utils import urns
from apache_beam.utils import windowed_value
from apache_beam.utils.timestamp import MIN_TIMESTAMP
from apache_beam.utils.timestamp import Duration
from apache_beam.utils.timestamp import Timestamp
from apache_beam.utils.windowed_value import WindowedValue

from apache_beam.transforms.window import *


class DynamicSessions(WindowFn):
  """A windowing function that groups elements into sessions.

  A session is defined as a series of consecutive events
  separated by a specified gap size.

  Attributes:
    gap_size: Size of the gap between windows as floating-point seconds.
  """

  def __init__(self, gap_size):
    if gap_size <= 0:
      raise ValueError('The size parameter must be strictly positive.')
    self.gap_size = Duration.of(gap_size)

  def assign(self, context):
    timestamp = context.timestamp

    try:
      gap = Duration.of(context.element[1]["gap"])
    except:
      gap = self.gap_size

    return [IntervalWindow(timestamp, timestamp + gap)]

  def get_window_coder(self):
    return coders.IntervalWindowCoder()

  def merge(self, merge_context):
    to_merge = []
    end = MIN_TIMESTAMP
    for w in sorted(merge_context.windows, key=lambda w: w.start):
      if to_merge:
        if end > w.start:
          to_merge.append(w)
          if w.end > end:
            end = w.end
        else:
          if len(to_merge) > 1:
            merge_context.merge(to_merge,
                                IntervalWindow(to_merge[0].start, end))
          to_merge = [w]
          end = w.end
      else:
        to_merge = [w]
        end = w.end
    if len(to_merge) > 1:
      merge_context.merge(to_merge, IntervalWindow(to_merge[0].start, end))

  def __eq__(self, other):
    if type(self) == type(other) == DynamicSessions:
      return self.gap_size == other.gap_size

  def __ne__(self, other):
    return not self == other

  def __hash__(self):
    return hash(self.gap_size)

  def to_runner_api_parameter(self, context):
    return (common_urns.session_windows.urn,
            standard_window_fns_pb2.SessionsPayload(
                gap_size=proto_utils.from_micros(
                    duration_pb2.Duration, self.gap_size.micros)))

  @urns.RunnerApiFn.register_urn(
      common_urns.session_windows.urn,
      standard_window_fns_pb2.SessionsPayload)
  def from_runner_api_parameter(fn_parameter, unused_context):
    return DynamicSessions(
        gap_size=Duration(micros=fn_parameter.gap_size.ToMicroseconds()))
