import argparse, logging, time
import inflect
 
import apache_beam as beam
import apache_beam.transforms.combiners as combine
from apache_beam.transforms.userstate import BagStateSpec
from apache_beam.coders import VarIntCoder
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
 
 
class LogFn(beam.DoFn):
  """Logs pairs"""
  def process(self, element):
    logging.info(element)
    yield element
 
 
class SplitFn(beam.DoFn):
  """Split sample"""
  def process(self, element):
    for elem in element:
      yield elem
 
class IndexAssigningStatefulDoFn(beam.DoFn):
  INDEX_STATE = BagStateSpec('index', VarIntCoder())
 
  def process(self, element, state=beam.DoFn.StateParam(INDEX_STATE)):
    unused_key, value = element
    next_index, = list(state.read()) or [0]
    yield (next_index, value)
    state.clear()
    state.add(next_index + 1)
        
 
def run(argv=None):
  NUM_ELEMENTS = 32

  parser = argparse.ArgumentParser()
  known_args, pipeline_args = parser.parse_known_args(argv)
 
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = True
  p = beam.Pipeline(options=pipeline_options)
 
  q = inflect.engine()
  data = [str(q.number_to_words(i)) for i in range(NUM_ELEMENTS)]
 
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
 
  result = p.run()
  result.wait_until_finish()
 
if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
