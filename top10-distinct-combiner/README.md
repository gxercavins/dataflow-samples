# Top 10 Distinct Combiner

The built-in `TopCombineFn` combine function can be used to calculate the top N users/keys according to a `compare` criterium that we specify. The problem, as evinced in this [StackOverflow question](https://stackoverflow.com/questions/56616576/apache-beam-python-how-to-get-the-top-10-elements-of-a-pcollection-with-accu/), is that, when using a triggering+accumulation strategy, old values from the same user can still appear in the top instead of being replaced by the new ones.

For example, Paul's score from older panes was still high enough so the same user appears several times in the top results:

```python
Paul - 38
Paul - 37
Michel - 27
Paul - 36
Michel - 26
...
(10 elements)
```

In some cases, we'd like this score to be updated and only display the latest per-user score:

```python
Paul - 38
Michel - 27
Kevin - 20
...
(10 elements)
```

## Quickstart

Set up [authentication](https://cloud.google.com/docs/authentication/) your preferred way and install the Beam Python package (use of `virtualenv` is recommended): `pip install apache-beam[gcp]`.

If you need to use the Pub/Sub message generator:
* Replace the `project_id` and `topic_name` variables at the top of the `generate_messages.py` file.
* Start with `python generate_messages.py` (and open a new shell if needed)

To execute the pipeline locally:
* Replace `PROJECT` and `TOPIC` variables in the `test_combine.py` file (when calling `ReadFromPubSub`).
* Start with `python test_combine.py --streaming`

Cancel local execution of both scripts when done. This code was tested with the `DirectRunner` and `apache-beam==2.13.0`. With `DataflowRunner` you'll probably need to add the [extra files](https://beam.apache.org/documentation/sdks/python-pipeline-dependencies/#multiple-file-dependencies) with a `setup.py` file.

The other two files, `top10.py` and `pubsub_top10.py` are prior tests to verify the behavior of the standard combiner using in-memory data and the generator, respectively. They are included in the repository but not discussed in this README.

## Example

One possibility is to transition to use [Discarding fired panes](https://beam.apache.org/documentation/programming-guide/#setting-a-trigger) instead, which can be set via `accumulation_mode=trigger.AccumulationMode.DISCARDING`. If our source expects to receive fired panes with the `ACCUMULATING` mode we'll want to modify `TopCombineFn` so that repeated panes from the same user overwrite the previous value and avoid duplicated keys. Our new combiner, `TopDistinctFn`, will be built upon the base code [here](https://github.com/apache/beam/blob/v2.13.0/sdks/python/apache_beam/transforms/combiners.py) for Beam SDK 2.13.0. In the `add_input` method we'll do a previous check as follows:

```python
for current_top_element in enumerate(heap):
  if element[0] == current_top_element[1].value[0]:
    heap[current_top_element[0]] = heap[-1]
    heap.pop()
    heapq.heapify(heap)
```

Basically, we'll compare the key for the element that we are evaluating (`element[0]`) versus each element in the heap. Heap elements are of type [`ComparableValue`](https://github.com/apache/beam/blob/v2.13.0/sdks/python/apache_beam/transforms/cy_combiners.py#L377) so we can use `value` to get back the tuple (and `value[0]` to get the key). If they match, we'll want to pop it out from the heap (as we are accumulating the sum will be greater). Beam SDK uses the [`heapq`](http://docs.python.org/library/heapq.html) library so I based my approach on [this answer](https://stackoverflow.com/a/10163422/6121516) to remove the `i-th` element (we use `enumerate` to keep index information). 

I also added some logging, which is commented out in the final version, to be able to detect duplicates:

```python
logging.info("Duplicate: " + element[0] + "," + str(element[1]) + ' --- ' + current_top_element[1].value[0] + ',' + str(current_top_element[1].value[1]))
```

The new code for the custom version of `TopCombineFn`, renamed `TopDistinctFn`, is located in a `top.py` file inside a `combiners` folder (with `__init__.py`) that we can import with:

```python
from combiners.top import TopDistinctFn
```

It might look overwhelming but I only added the few lines of code, that I already explained, starting at line 425. Then, we can use our `TopDistinctFn` from within the pipeline as shown below:

```python
(inputs
     | 'Add User as key' >> beam.Map(lambda x: (x, 1)) # ('key', 1)
     | 'Apply Window of time' >> beam.WindowInto(
                    beam.window.FixedWindows(size=10*60),
                    trigger=beam.trigger.Repeatedly(beam.trigger.AfterCount(2)),
                    accumulation_mode=beam.trigger.AccumulationMode.ACCUMULATING)
     | 'Sum Score' >> beam.CombinePerKey(sum)   
     | 'Top 10 scores' >> beam.CombineGlobally(
                    TopDistinctFn(n=10, compare=lambda a, b: a[1] < b[1])).without_defaults()
     | 'Print results' >> beam.ParDo(PrintTop10Fn()))
```

As an example, `Bob` was leading with 9 points and, when the next update comes, his score is up to 11 points. He'll appear in the next recap with only the updated score and no duplicate (as detected via our logging). The entry with 9 points will not appear anymore and the top will still have 10 users as desired. Likewise for `Marta`. I noted that older scores still appear in the heap even if not in the top 10 but I am not sure how garbage collection works with `heapq`.

```
INFO:root:>>> Current top 10: [('Bob', 9), ('Connor', 8), ('Eva', 7), ('Hugo', 7), ('Paul', 6), ('Kevin', 6), ('Laura', 6), ('Marta', 6), ('Diane', 4), ('Bacon', 4)]
...
INFO:root:Duplicate: Marta,8 --- Marta,6
INFO:root:Duplicate: Bob,11 --- Bob,9
INFO:root:>>> Current top 10: [('Bob', 11), ('Connor', 8), ('Marta', 8), ('Bacon', 7), ('Eva', 7), ('Hugo', 7), ('Paul', 6), ('Laura', 6), ('Diane', 6), ('Kevin', 6)]
```

## License

This example is provided under the Apache License 2.0.

## Issues

Report any issue to the GitHub issue tracker.
