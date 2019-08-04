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

"""A library of basic combiner PTransform subclasses."""

from __future__ import absolute_import
from __future__ import division

import heapq
import operator
import logging
import random
import sys
import warnings
from builtins import object
from builtins import zip

from past.builtins import long

from apache_beam.transforms import core
from apache_beam.transforms import cy_combiners
from apache_beam.transforms import ptransform
from apache_beam.transforms import window
from apache_beam.transforms.display import DisplayDataItem
from apache_beam.typehints import KV
from apache_beam.typehints import Any
from apache_beam.typehints import Dict
from apache_beam.typehints import Iterable
from apache_beam.typehints import List
from apache_beam.typehints import Tuple
from apache_beam.typehints import TypeVariable
from apache_beam.typehints import Union
from apache_beam.typehints import with_input_types
from apache_beam.typehints import with_output_types

__all__ = [
    'Top',
    ]

# Type variables
T = TypeVariable('T')
K = TypeVariable('K')
V = TypeVariable('V')


class Top(object):
  """Combiners for obtaining extremal elements."""
  # pylint: disable=no-self-argument

  class Of(ptransform.PTransform):
    """Obtain a list of the compare-most N elements in a PCollection.

    This transform will retrieve the n greatest elements in the PCollection
    to which it is applied, where "greatest" is determined by the comparator
    function supplied as the compare argument.
    """

    def _py2__init__(self, n, compare=None, *args, **kwargs):
      """Initializer.

      compare should be an implementation of "a < b" taking at least two
      arguments (a and b). Additional arguments and side inputs specified in
      the apply call become additional arguments to the comparator. Defaults to
      the natural ordering of the elements.
      The arguments 'key' and 'reverse' may instead be passed as keyword
      arguments, and have the same meaning as for Python's sort functions.

      Args:
        pcoll: PCollection to process.
        n: number of elements to extract from pcoll.
        compare: as described above.
        *args: as described above.
        **kwargs: as described above.
      """
      if compare:
        warnings.warn('Compare not available in Python 3, use key instead.',
                      DeprecationWarning)
      self._n = n
      self._compare = compare
      self._key = kwargs.pop('key', None)
      self._reverse = kwargs.pop('reverse', False)
      self._args = args
      self._kwargs = kwargs

    def _py3__init__(self, n, **kwargs):
      """Creates a global Top operation.

      The arguments 'key' and 'reverse' may be passed as keyword arguments,
      and have the same meaning as for Python's sort functions.

      Args:
        pcoll: PCollection to process.
        n: number of elements to extract from pcoll.
        **kwargs: may contain 'key' and/or 'reverse'
      """
      unknown_kwargs = set(kwargs.keys()) - set(['key', 'reverse'])
      if unknown_kwargs:
        raise ValueError(
            'Unknown keyword arguments: ' + ', '.join(unknown_kwargs))
      self._py2__init__(n, None, **kwargs)

    # Python 3 sort does not accept a comparison operator, and nor do we.
    if sys.version_info[0] < 3:
      __init__ = _py2__init__
    else:
      __init__ = _py3__init__

    def default_label(self):
      return 'Top(%d)' % self._n

    def expand(self, pcoll):
      compare = self._compare
      if (not self._args and not self._kwargs and
          pcoll.windowing.is_default()):
        if self._reverse:
          if compare is None or compare is operator.lt:
            compare = operator.gt
          else:
            original_compare = compare
            compare = lambda a, b: original_compare(b, a)
        # This is a more efficient global algorithm.
        top_per_bundle = pcoll | core.ParDo(
            _TopPerBundle(self._n, compare, self._key))
        # If pcoll is empty, we can't guerentee that top_per_bundle
        # won't be empty, so inject at least one empty accumulator
        # so that downstream is guerenteed to produce non-empty output.
        empty_bundle = pcoll.pipeline | core.Create([(None, [])])
        return (
            (top_per_bundle, empty_bundle) | core.Flatten()
            | core.GroupByKey()
            | core.ParDo(_MergeTopPerBundle(self._n, compare, self._key)))
      else:
        return pcoll | core.CombineGlobally(
            TopDistinctFn(self._n, compare, self._key, self._reverse),
            *self._args, **self._kwargs)

  class PerKey(ptransform.PTransform):
    """Identifies the compare-most N elements associated with each key.

    This transform will produce a PCollection mapping unique keys in the input
    PCollection to the n greatest elements with which they are associated, where
    "greatest" is determined by the comparator function supplied as the compare
    argument in the initializer.
    """
    def _py2__init__(self, n, compare=None, *args, **kwargs):
      """Initializer.

      compare should be an implementation of "a < b" taking at least two
      arguments (a and b). Additional arguments and side inputs specified in
      the apply call become additional arguments to the comparator.  Defaults to
      the natural ordering of the elements.

      The arguments 'key' and 'reverse' may instead be passed as keyword
      arguments, and have the same meaning as for Python's sort functions.

      Args:
        n: number of elements to extract from input.
        compare: as described above.
        *args: as described above.
        **kwargs: as described above.
      """
      if compare:
        warnings.warn('Compare not available in Python 3, use key instead.',
                      DeprecationWarning)
      self._n = n
      self._compare = compare
      self._key = kwargs.pop('key', None)
      self._reverse = kwargs.pop('reverse', False)
      self._args = args
      self._kwargs = kwargs

    def _py3__init__(self, n, **kwargs):
      """Creates a per-key Top operation.

      The arguments 'key' and 'reverse' may be passed as keyword arguments,
      and have the same meaning as for Python's sort functions.

      Args:
        pcoll: PCollection to process.
        n: number of elements to extract from pcoll.
        **kwargs: may contain 'key' and/or 'reverse'
      """
      unknown_kwargs = set(kwargs.keys()) - set(['key', 'reverse'])
      if unknown_kwargs:
        raise ValueError(
            'Unknown keyword arguments: ' + ', '.join(unknown_kwargs))
      self._py2__init__(n, None, **kwargs)

    # Python 3 sort does not accept a comparison operator, and nor do we.
    if sys.version_info[0] < 3:
      __init__ = _py2__init__
    else:
      __init__ = _py3__init__

    def default_label(self):
      return 'TopPerKey(%d)' % self._n

    def expand(self, pcoll):
      """Expands the transform.

      Raises TypeCheckError: If the output type of the input PCollection is not
      compatible with KV[A, B].

      Args:
        pcoll: PCollection to process

      Returns:
        the PCollection containing the result.
      """
      return pcoll | core.CombinePerKey(
          TopDistinctFn(self._n, self._compare, self._key, self._reverse),
          *self._args, **self._kwargs)

  @staticmethod
  @ptransform.ptransform_fn
  def Largest(pcoll, n):
    """Obtain a list of the greatest N elements in a PCollection."""
    return pcoll | Top.Of(n)

  @staticmethod
  @ptransform.ptransform_fn
  def Smallest(pcoll, n):
    """Obtain a list of the least N elements in a PCollection."""
    return pcoll | Top.Of(n, reverse=True)

  @staticmethod
  @ptransform.ptransform_fn
  def LargestPerKey(pcoll, n):
    """Identifies the N greatest elements associated with each key."""
    return pcoll | Top.PerKey(n)

  @staticmethod
  @ptransform.ptransform_fn
  def SmallestPerKey(pcoll, n, reverse=True):
    """Identifies the N least elements associated with each key."""
    return pcoll | Top.PerKey(n, reverse=True)


@with_input_types(T)
@with_output_types(KV[None, List[T]])
class _TopPerBundle(core.DoFn):
  def __init__(self, n, less_than, key):
    self._n = n
    self._less_than = None if less_than is operator.le else less_than
    self._key = key

  def start_bundle(self):
    self._heap = []

  def process(self, element):
    if self._less_than or self._key:
      element = cy_combiners.ComparableValue(
          element, self._less_than, self._key)
    if len(self._heap) < self._n:
      heapq.heappush(self._heap, element)
    else:
      heapq.heappushpop(self._heap, element)

  def finish_bundle(self):
    # Though sorting here results in more total work, this allows us to
    # skip most elements in the reducer.
    # Essentially, given s map bundles, we are trading about O(sn) compares in
    # the (single) reducer for O(sn log n) compares across all mappers.
    self._heap.sort()

    # Unwrap to avoid serialization via pickle.
    if self._less_than or self._key:
      yield window.GlobalWindows.windowed_value(
          (None, [wrapper.value for wrapper in self._heap]))
    else:
      yield window.GlobalWindows.windowed_value(
          (None, self._heap))


@with_input_types(KV[None, Iterable[List[T]]])
@with_output_types(List[T])
class _MergeTopPerBundle(core.DoFn):
  def __init__(self, n, less_than, key):
    self._n = n
    self._less_than = None if less_than is operator.lt else less_than
    self._key = key

  def process(self, key_and_bundles):
    _, bundles = key_and_bundles
    heap = []
    for bundle in bundles:
      if not heap:
        if self._less_than or self._key:
          heap = [
              cy_combiners.ComparableValue(element, self._less_than, self._key)
              for element in bundle]
        else:
          heap = bundle
        continue

      for element in reversed(bundle):
        if self._less_than or self._key:
          element = cy_combiners.ComparableValue(
              element, self._less_than, self._key)
        if len(heap) < self._n:
          heapq.heappush(heap, element)
        elif element < heap[0]:
          # Because _TopPerBundle returns sorted lists, all other elements
          # will also be smaller.
          break
        else:
          heapq.heappushpop(heap, element)

    heap.sort()
    if self._less_than or self._key:
      yield [wrapper.value for wrapper in reversed(heap)]
    else:
      yield heap[::-1]


@with_input_types(T)
@with_output_types(List[T])
class TopDistinctFn(core.CombineFn):
  """CombineFn doing the combining for all of the Top transforms.

  This CombineFn uses a key or comparison operator to rank the elements.

  Args:
    compare: (optional) an implementation of "a < b" taking at least two
        arguments (a and b). Additional arguments and side inputs specified
        in the apply call become additional arguments to the comparator.
    key: (optional) a mapping of elements to a comparable key, similar to
        the key argument of Python's sorting methods.
    reverse: (optional) whether to order things smallest to largest, rather
        than largest to smallest
  """

  # TODO(robertwb): For Python 3, remove compare and only keep key.
  def __init__(self, n, compare=None, key=None, reverse=False):
    self._n = n

    if compare is operator.lt:
      compare = None
    elif compare is operator.gt:
      compare = None
      reverse = not reverse

    if compare:
      self._compare = (
          (lambda a, b, *args, **kwargs: not compare(a, b, *args, **kwargs))
          if reverse
          else compare)
    else:
      self._compare = operator.gt if reverse else operator.lt

    self._less_than = None
    self._key = key

  def _hydrated_heap(self, heap):
    if heap:
      first = heap[0]
      if isinstance(first, cy_combiners.ComparableValue):
        if first.requires_hydration:
          assert self._less_than is not None
          for comparable in heap:
            assert comparable.requires_hydration
            comparable.hydrate(self._less_than, self._key)
            assert not comparable.requires_hydration
          return heap
        else:
          return heap
      else:
        assert self._less_than is not None
        return [
            cy_combiners.ComparableValue(element, self._less_than, self._key)
            for element in heap
        ]
    else:
      return heap

  def display_data(self):
    return {'n': self._n,
            'compare': DisplayDataItem(self._compare.__name__
                                       if hasattr(self._compare, '__name__')
                                       else self._compare.__class__.__name__)
                       .drop_if_none()}

  # The accumulator type is a tuple
  # (bool, Union[List[T], List[ComparableValue[T]])
  # where the boolean indicates whether the second slot contains a List of T
  # (False) or List of ComparableValue[T] (True). In either case, the List
  # maintains heap invariance. When the contents of the List are
  # ComparableValue[T] they either all 'requires_hydration' or none do.
  # This accumulator representation allows us to minimize the data encoding
  # overheads. Creation of ComparableValues is elided for performance reasons
  # when there is no need for complicated comparison functions.
  def create_accumulator(self, *args, **kwargs):
    return (False, [])

  def add_input(self, accumulator, element, *args, **kwargs):
    # Caching to avoid paying the price of variadic expansion of args / kwargs
    # when it's not needed (for the 'if' case below).
    if self._less_than is None:
      if args or kwargs:
        self._less_than = lambda a, b: self._compare(a, b, *args, **kwargs)
      else:
        self._less_than = self._compare

    holds_comparables, heap = accumulator
    if self._less_than is not operator.lt or self._key:
      heap = self._hydrated_heap(heap)
      holds_comparables = True
    else:
      assert not holds_comparables

    # this is the new part of code
    for current_top_element in enumerate(heap):
      if element[0] == current_top_element[1].value[0]:
        # logging.info("Duplicate: " + element[0] + "," + str(element[1]) + ' --- ' + current_top_element[1].value[0] + ',' + str(current_top_element[1].value[1]))
        heap[current_top_element[0]] = heap[-1]
        heap.pop()
        heapq.heapify(heap)

    comparable = (
        cy_combiners.ComparableValue(element, self._less_than, self._key)
        if holds_comparables else element)

    if len(heap) < self._n:
      heapq.heappush(heap, comparable)
    else:
      heapq.heappushpop(heap, comparable)
    return (holds_comparables, heap)

  def merge_accumulators(self, accumulators, *args, **kwargs):
    if args or kwargs:
      self._less_than = lambda a, b: self._compare(a, b, *args, **kwargs)
      add_input = lambda accumulator, element: self.add_input(
          accumulator, element, *args, **kwargs)
    else:
      self._less_than = self._compare
      add_input = self.add_input

    result_heap = None
    holds_comparables = None
    for accumulator in accumulators:
      holds_comparables, heap = accumulator
      if self._less_than is not operator.lt or self._key:
        heap = self._hydrated_heap(heap)
        holds_comparables = True
      else:
        assert not holds_comparables

      if result_heap is None:
        result_heap = heap
      else:
        for comparable in heap:
          _, result_heap = add_input(
              (holds_comparables, result_heap),
              comparable.value if holds_comparables else comparable)

    assert result_heap is not None and holds_comparables is not None
    return (holds_comparables, result_heap)

  def compact(self, accumulator, *args, **kwargs):
    holds_comparables, heap = accumulator
    # Unwrap to avoid serialization via pickle.
    if holds_comparables:
      return (False, [comparable.value for comparable in heap])
    else:
      return accumulator

  def extract_output(self, accumulator, *args, **kwargs):
    if args or kwargs:
      self._less_than = lambda a, b: self._compare(a, b, *args, **kwargs)
    else:
      self._less_than = self._compare

    holds_comparables, heap = accumulator
    if self._less_than is not operator.lt or self._key:
      if not holds_comparables:
        heap = self._hydrated_heap(heap)
        holds_comparables = True
    else:
      assert not holds_comparables

    assert len(heap) <= self._n
    heap.sort(reverse=True)
    return [
        comparable.value if holds_comparables else comparable
        for comparable in heap
    ]
