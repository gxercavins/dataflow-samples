# Normalize PCollection values

The idea is to group all values by the same key and normalize them. In order to do so, we need to do a first pass through the data to find the maximum and minimum per each key, join them back with the data and finally calculate the normalized values as:
``` 
norm_value = (value - min) / (max - min)
```

Solution written as an answer to a [StackOverflow question](https://stackoverflow.com/questions/49756539/max-and-min-for-several-fields-inside-pcollection-in-apache-beam-with-python).

## Quickstart

Set up [authentication](https://cloud.google.com/docs/authentication/) your preferred way and install the following Python packages (use of `virtualenv` is recommended):
* `pip install apache-beam`
* `pip install apache-beam[gcp]`

To test the script locally, provide an output file: `python normalize-values.py --output output.txt`

This code was tested with `apache-beam==2.5.0`.

## Example

The test data that we will use is:
``` python
[('foo', 1), ('bar', 5), ('foo', 5), ('bar', 9), ('bar', 2)]
```
And, grouped by key:
``` python
('foo', [1, 5])
('bar', [5, 9, 2])
```
I wrote a custom [`CombineFn`](https://beam.apache.org/documentation/programming-guide/#core-beam-transforms) function to determine the minimum and maximum per each key:

``` python
# custom CombineFn that outputs min and max value
class MinMaxFn(beam.CombineFn):
  # initialize min and max values (I assumed int type)
  def create_accumulator(self):
    return (sys.maxint, 0)

  # update if current value is a new min or max      
  def add_input(self, min_max, input):
    (current_min, current_max) = min_max
    return min(current_min, input), max(current_max, input)

  def merge_accumulators(self, accumulators):
    return accumulators

  def extract_output(self, min_max):
    return min_max
```

The CombineFn will return per key min and max, in our particular example:
``` python
('foo', [(1, 5)])
('bar', [(2, 9)])
```
Then, merge them  together with the input data using `CoGroupByKey`:
``` python
('foo', ([1, 5], [[(1, 5)]]))
('bar', ([5, 9, 2], [[(2, 9)]]))
```
Finally, we apply the desired mapping to normalize the values, taking into account the input and output schemas:
``` python
beam.Map(lambda (a, (b, c)): (a, [float(val - c[0][0][0])/(c[0][0][1] -c[0][0][0]) for val in b]))
```
And after normalizing we obtain the desired output:
``` python
('foo', [0.0, 1.0])
('bar', [0.42857142857142855, 1.0, 0.0])
```

## License

These examples are provided under the Apache License 2.0.

## Issues

Report any issue to the GitHub issue tracker.
