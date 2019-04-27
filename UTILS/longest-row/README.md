# Longest row

In this example, originally written as an answer to a [StackOverflow question](https://stackoverflow.com/questions/55607111/find-string-with-max-number-of-tokens-using-apache-beams-python-sdk), we want to find the line with the greatest number of words/tokens.

## Quickstart

Set up [authentication](https://cloud.google.com/docs/authentication/) your preferred way and install the Beam Python package (use of `virtualenv` is recommended): `pip install apache-beam[gcp]==2.11.0`.

The script can be tested locally with: `python top.py`.

To run on Google Cloud Platform set up the `PROJECT` and `BUCKET` env variables and execute: `python top.py --runner DataflowRunner --temp_location gs://$BUCKET/temp --project $PROJECT`.

This code was tested with the Direct Runner and `apache-beam==2.11.0`.

## Example

To obtain the desired result we can use the [`Top.of`][1] transform. Briefly, we split each sentence into words and then calculate the token length. With `Top` we just want the number one result (hence the 1 as the argument) and we pass as second argument a lambda function as the comparison criteria to sort them by word length (`lambda a,b: a[1]<b[1]`):

```python
sentences = sentences = ['This is the first sentence',
                         'Second sentence',
                         'Yet another sentence']

longest_sentence = (p
  | 'Read Sentences' >> beam.Create(sentences)
  | 'Split into Words' >> beam.Map(lambda x: x.split(' '))
  | 'Map Token Length'      >> beam.Map(lambda x: (x, len(x)))
  | 'Top Sentence' >> combine.Top.Of(1, lambda a,b: a[1]<b[1])
  | 'Save Variable'         >> beam.ParDo(SaveMaxFn()))
```

where `SaveMaxFn()`is:

```python
class SaveMaxFn(beam.DoFn):
  """Stores max in global variable and prints result"""
  def process(self, element):
    length = element[0][1]
    logging.info("Longest sentence: %s tokens", length)

    return element
```

output, as expected, is:

    INFO:root:Longest sentence: 5 token(s)


## License

This example is provided under the Apache License 2.0.

## Issues

Report any issue to the GitHub issue tracker.
