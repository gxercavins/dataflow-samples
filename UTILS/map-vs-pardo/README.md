# Map vs ParDo with the Python SDK

This example shows how to call functions in two different ways: using `Beam.Map` and `Beam.ParDo`. Originally written as an answer to a StackOverflow [question](https://stackoverflow.com/questions/56502093/why-map-works-and-pardo-doesnt/).

## Quickstart

* Set up [authentication](https://cloud.google.com/docs/authentication/) your preferred way
* Install the `apache-beam[gcp]` Python package: `apache-beam[gcp]` (use of `virtualenv` is recommended)
* Execute `python map_vs_pardo.py`

This code was tested locally with `apache-beam[gcp]==2.12.0`.

## Example

When using `Beam.Map` we can provide inline a lambda function that calls the `compute_interest_map` function for every element. Instead, `Beam.ParDo` expects a to call a `Beam.DoFn` function with a `process` method:

```python
def process(self, element):
```
As explained in section 4.2.1.2 of the [Beam programming guide](https://beam.apache.org/documentation/programming-guide/#core-beam-transforms):

> Inside your DoFn subclass, you’ll write a method process where you provide the actual processing logic. You don’t need to manually extract the elements from the input collection; the Beam SDKs handle that for you. Your process method should accept an object of type element. This is the input element and output is emitted by using yield or return statement inside process method.

In our example we define both `Map` and `ParDo` functions as:

```python
def compute_interest_map(data_item):
  return data_item + 1

class compute_interest_pardo(beam.DoFn):
  def process(self, element):
    yield element + 2
```

If we rename `process` for another method name we get a `NotImplementedError`. 

And the main pipeline to test them will be:

```python
events = (p
  | 'Create' >> beam.Create([1, 2, 3]) \
  | 'Add 1' >> beam.Map(lambda x: compute_interest_map(x)) \
  | 'Add 2' >> beam.ParDo(compute_interest_pardo()) \
  | 'Print' >> beam.ParDo(log_results()))
```

Which yields the expected output:

```python
INFO:root:>> Interest: 4
INFO:root:>> Interest: 5
INFO:root:>> Interest: 6
```

## License

These examples are provided under the Apache License 2.0.

## Issues

Report any issue to the GitHub issue tracker.
