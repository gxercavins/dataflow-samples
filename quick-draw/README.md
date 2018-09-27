# 'Quick, Draw!' Dataset Playground

The main idea of this example is to use Dataflow to explore the [Quick, Draw!](https://towardsdatascience.com/quick-draw-the-worlds-largest-doodle-dataset-823c22ffce6b) dataset. This is a Google initiative to collect user-made doodles belonging to different categories such as places, animals, etc. It can be accessed through the [GitHub page](https://github.com/googlecreativelab/quickdraw-dataset) and it's also hosted on a public [GCS Bucket](https://console.cloud.google.com/storage/browser/quickdraw_dataset). Projects revolving around this dataset have been focused on Machine Learning and trying to predict what is being sketched (see the game [here](https://quickdraw.withgoogle.com/#)). In our case, we are going to focus on the [owl drawings'](https://quickdraw.withgoogle.com/data/owl) raw data, convert them to images and save them in `png` format (147,654 files). We'll use the Python SDK for the convenience to use libraries such as `json` or `Pillow`.

![screenshot from 2018-09-02 20-16-55](https://user-images.githubusercontent.com/29493411/44959774-8fa4de00-aef4-11e8-9516-448117a52ebc.png)

## Quickstart

Set up [authentication](https://cloud.google.com/docs/authentication/) your preferred way and install the following Python packages (use of `virtualenv` is recommended):
* `pip install apache-beam`
* `pip install apache-beam[gcp]`
* `pip install numpy`
* `pip install pillow`

Alternatively, install from the `requirements.txt` file provided with: `pip install -r requirements.txt`

To test the script locally, provide an output file: `python convert.py --output output.txt`.

To run on Google Cloud Platform set up the `PROJECT` and `BUCKET` env variables and execute: `python convert.py --runner DataflowRunner --temp_location gs://$BUCKET/temp --project $PROJECT --output gs://$BUCKET/owls/images/`.
As of now, Dataflow workers have `numpy==1.13.3` and `pillow==3.4.1` already installed as [extra dependencies](https://cloud.google.com/dataflow/docs/concepts/sdk-worker-dependencies#sdk-for-python) so there is no need to use the `--requirements_file` flag.

This code was tested with `apache-beam==2.6.0`.

## Example

Data is already pre-processed and available in `ndjson` format. We can explore the first element, which belongs to a single owl doodle, using the following command:

```bash
gsutil cat gs://quickdraw_dataset/full/simplified/owl.ndjson | head -n 1
```

This will return the following:

```json
{
    "word":"owl",
    "countrycode":"US",
    "timestamp":"2017-03-04 23:38:44.30111 UTC",
    "recognized":true,"key_id":"6329943778656256",
    "drawing":[
        [[56,30,9,0,4,15,26,62,94,111,118,143,152,149,143,129,108,86,68,46,33,28],[19,29,51,75,106,133,143,151,149,144,139,102,66,34,24,14,5,0,1,7,16,25]],
        [[45,32,26,26,36,47,56,65,69,64,56,37,30,30],[45,53,67,90,100,101,95,79,59,48,42,37,37,40]],
        [[109,92,83,82,88,96,112,122,125,121,112],[45,57,71,87,101,104,101,87,55,46,48]],
        [[66,73,69,67],[97,98,108,99]],
        [[47,47,51,51,46,43,47,50,45,42,44,48,48],[67,71,70,66,68,76,76,69,68,71,75,75,69]],
        [[107,100,101,105,110,108,103,102,106,107,105],[69,74,78,80,74,70,75,78,78,73,77]],
        [[38,20,14,4],[14,10,28,40]],
        [[128,141,151,150],[13,0,19,37]],
        [[49,41,50,65,85,98,117,132,144,153,152,143,137,120],[150,193,226,239,250,255,254,249,236,194,164,142,134,132]]
    ]
}
```

`word` will be owl in all our examples. We also have the `countrycode` and `timestamp`, which could be used in conjunction with `recognized` to compare drawing skills across countries or along time. `key_id` will be the unique identifier of each drawing, which is presented as an array of strokes in the `drawing` field. For additional details, format is described [here](https://github.com/googlecreativelab/quickdraw-dataset#the-raw-moderated-dataset).

If we want to select a specific field we can do so in a pipeline such as:

```python
input = p | 'read' >> ReadFromText("gs://quickdraw_dataset/full/simplified/owl.ndjson")
jsons = input | 'json' >> beam.Map(lambda x: json.loads(x))
lines = jsons | 'map' >> beam.Map(lambda x: (x['countrycode'], x['timestamp']))
```

which would output the following:
```python
(u'US', u'2017-03-04 23:38:44.30111 UTC')
(u'MX', u'2017-03-09 14:55:37.98255 UTC')
(u'NL', u'2017-01-02 10:19:12.96254 UTC')
(u'GB', u'2017-03-13 22:31:02.86944 UTC')
(u'US', u'2017-03-07 23:14:24.04772 UTC')
...
```

The `EncodeFn` function, based on [this example](https://colab.research.google.com/github/tensorflow/workshops/blob/master/extras/amld/notebooks/exercises/1_qd_data.ipynb#scrollTo=EBkp94O9GeFt), will create a blank image (default is 128x128, replace `img_size` if needed), generate a Numpy array containing all the strokes in the `drawing` field and draw them. Finally, byte data will be converted to the desired format and we'll output a tuple containing the identifier or `key_id` and the `image`.

```python
class EncodeFn(beam.DoFn):
    def process(self, element, img_size=128, lw=3):
        img = Image.new('RGB', (img_size, img_size), (255,255,255))
        draw = ImageDraw.Draw(img)
        element['drawing'] = [np.array(stroke) for stroke in element['drawing']]
        lines = np.array([
            stroke[0:2, i:i+2]
            for stroke in element['drawing']
            for i in range(stroke.shape[1] - 1)
        ], dtype=np.float32)
        lines /= 1024
        for line in lines:
            draw.line(tuple(line.T.reshape((-1,)) * img_size), fill='black', width=lw)
        b = io.BytesIO()
        img.convert('RGB').save(b, format='png')
        yield {'key_id':element['key_id'], 'image':b.getvalue()}
```

Alternatively, we could encode them to `Base64` to, for example, pass them inline in an API request:

```python
...
b = io.BytesIO()
img.convert('RGB').save(b, format='png')
enc = base64.b64encode(b.getvalue())
yield enc
```

We want to write the images into individual files to GCS. We could specify `num_shards` in `WriteToText` and a large enough number (larger than 147,654) to ensure each one goes to a different shard but: a) that's not an elegant solution and b) we have more control with `filesystems.FileSystems.create`. We'll use the `key_id` as the name of the file and append the `.png` suffix:

```python
class WriteToSeparateFiles(beam.DoFn):
    def __init__(self, outdir):
        self.outdir = outdir
    def process(self, element):
        writer = filesystems.FileSystems.create(self.outdir + element['key_id'] + '.png')
        writer.write(element['image'])
        writer.close()
```

Some of the resulting images after downloading them from the bucket:

![screenshot from 2018-09-02 21-00-46](https://user-images.githubusercontent.com/29493411/44959776-96cbec00-aef4-11e8-902a-0bcb71c713a1.png)

## License

This example is provided under the Apache License 2.0.

## Issues

Report any issue to the GitHub issue tracker.
