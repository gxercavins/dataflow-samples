## Zip a PCollection

Main idea here is to create random pairs of elements from a PCollection as in StackOverflow [question](https://stackoverflow.com/questions/55485228/is-it-possible-to-do-a-zip-operation-in-apache-beam-on-two-pcollections/). Something akin to creating a copy of the PCollection, applying a 
random shuffle and zipping it back to the original PCollection.

Things get complicated here because there is no order guarantee in Dataflow. However, I played around with a couple samples to get the desired output. `zip1.py` contains a different approach where we shuffle the input PCollection and then pair consecutive elements based on [state][1].

For this I have used the Python SDK but the Dataflow Runner does not support stateful DoFn's yet. It works with the Direct Runner but: 1) it is not scalable and 2) it's difficult to shuffle the records without multi-threading. Of course, an easy solution for the latter is to feed an already shuffled PCollection to the pipeline (we can use a different job to pre-process the data). Otherwise, this example can be adapted to the Java SDK.

For now, I decided to try to shuffle and pair it with a single pipeline. Briefly, the stateful DoFn looks at the buffer and, if it is empty, it saves the current element. Otherwise, it pops out the previous element from the buffer and outputs a tuple of `(previous_element, current_element)`:

```python
class PairRecordsFn(beam.DoFn):
  """Pairs two consecutive elements after shuffle"""
  BUFFER = BagStateSpec('buffer', PickleCoder())
  def process(self, element, buffer=beam.DoFn.StateParam(BUFFER)):
    try:
      previous_element = list(buffer.read())[0]
    except:
      previous_element = []
    unused_key, value = element

    if previous_element:
      yield (previous_element, value)
      buffer.clear()
    else:
      buffer.add(value)
```

The pipeline adds keys to the input elements as it is a requirement to use stateful DoFns. Therefore, there will be a trade-off because you can potentially assign the same key to all elements with `beam.Map(lambda x: (1, x))` but this would not parallelize well (a single worker will handle all elements). It's not really a problem here as we are using the Direct Runner anyway (keep it in mind if using the Java SDK, though). 

However, it will not shuffle the records. If, instead, we shuffle to a large amount of keys we'll get a larger number of "orphaned" elements that can't be paired (as state is preserved per key and we assign them randomly we can have an odd number of records per key):

```python
pairs = (p
  | 'Create Events' >> beam.Create(data)
  | 'Add Keys' >> beam.Map(lambda x: (randint(1,4), x))
  | 'Pair Records' >> beam.ParDo(PairRecordsFn())
  | 'Check Results' >> beam.ParDo(LogFn()))
```

In my case I got something like (weak shuffle):

```python
INFO:root:('one', 'three')
INFO:root:('two', 'five')
INFO:root:('zero', 'six')
INFO:root:('four', 'seven')
INFO:root:('ten', 'twelve')
INFO:root:('nine', 'thirteen')
INFO:root:('eight', 'fourteen')
INFO:root:('eleven', 'sixteen')
```

`zip2.py` contains another way to do it, similar to the one in the question description, using the `Sample.FixedSizeGlobally` combiner. The good thing is that it shuffles the data better but we need to know the number of elements, `NUM_ELEMENTS`, a priori (otherwise we'd need an initial pass on the data or using `Count`) and it seems to return all elements together. Briefly, we initialize the same PCollection twice but apply different shuffle orders and assign indexes in a stateful DoFn. This will guarantee that indexes are unique across elements in the same PCollection (even if no order is guaranteed). In my case, both PCollections will have exactly one record for each key in the range [0, 31]. A CoGroupByKey transform will join both PCollections on the same index thus having random pairs of elements (1:1 mappings):

```python
pc1 = (p
  | 'Create Events 1' >> beam.Create(data)
  | 'Sample 1' >> combine.Sample.FixedSizeGlobally(NUM_ELEMENTS)
  | 'Split Sample 1' >> beam.ParDo(SplitFn())
  | 'Add Dummy Key 1' >> beam.Map(lambda x: (1, x))
  | 'Assign Index 1' >> beam.ParDo(IndexAssigningStatefulDoFn()))
  
pc2 = (p
  | 'Create Events 2' >> beam.Create(data)
  | 'Sample 2' >> combine.Sample.FixedSizeGlobally(NUM_ELEMENTS)
  | 'Split Sample 2' >> beam.ParDo(SplitFn())
  | 'Add Dummy Key 2' >> beam.Map(lambda x: (2, x))
  | 'Assign Index 2' >> beam.ParDo(IndexAssigningStatefulDoFn()))

zipped = ((pc1, pc2)
           | 'Zip Shuffled PCollections' >> beam.CoGroupByKey()
           | 'Drop Index' >> beam.Map(lambda (x, y):y)
           | 'Check Results' >> beam.ParDo(LogFn()))
```

Results (zipped and better shuffled):
```python
    INFO:root:(['ten'], ['nineteen'])
    INFO:root:(['twenty-three'], ['seven'])
    INFO:root:(['twenty-five'], ['twenty'])
    INFO:root:(['twelve'], ['twenty-one'])
    INFO:root:(['twenty-six'], ['twenty-five'])
    INFO:root:(['zero'], ['twenty-three'])
    ...
```

  [1]: https://beam.apache.org/blog/2017/02/13/stateful-processing.html

Tested with Beam 2.9.0 SDK (see `requirements.txt`).

## License

These examples are provided under the Apache License 2.0.

## Issues

Report any issue to the GitHub issue tracker.
