# GCS to Datastore

## Parsing text with a JavaScript UDF

Example that showcases how to provide a custom transform function, written in JavaScript, to parse an input JSON and write it as a [Datastore entity][1]. Originally written as a [StackOverflow answer](https://stackoverflow.com/a/60217774/6121516).

## Example

The following snippet, which can be executed directly in the StackOverflow page, will serve as test of our input data and desired output:

```js
var data = {
  "userId": "u-skjbdw34jh3gx",
  "rowRanks": [
    {
      "originalTrigger": "recent",
      "programmedRowPos": "VR1",
      "reorderedRowPos": 0
    },
    {
      "originalTrigger": "discovery",
      "programmedRowPos": "VR1",
      "reorderedRowPos": 1
    }
  ]
}

var entity = {};
entity.key = {};
entity.key.partitionId = {};
entity.key.partitionId.projectId = "gcp-project-id";
entity.key.partitionId.namespaceId = "spring-demo";

var path = {}
path.kind = "demo";
path.name = "userId";
entity.key.path = [];
entity.key.path.push(path);

entity.properties = {};
entity.properties.userId = {};
entity.properties.userId.stringValue = data.userId;
entity.properties.rowRanks = {};
entity.properties.rowRanks.arrayValue = {};

var arrayValues = [];
data.rowRanks.forEach(buildArrayValue);

function buildArrayValue(row) {
  var temp = {};
  temp.entityValue = {};
  temp.entityValue.properties = {};
  temp.entityValue.properties.originalTrigger = {};
  temp.entityValue.properties.originalTrigger.stringValue = row.originalTrigger;
  temp.entityValue.properties.programmedRowPos = {};
  temp.entityValue.properties.programmedRowPos.stringValue = row.programmedRowPos;
  temp.entityValue.properties.reorderedRowPos = {};
  temp.entityValue.properties.reorderedRowPos.integerValue = row.reorderedRowPos;
  arrayValues.push(temp);
}

entity.properties.rowRanks.arrayValue.values = arrayValues;

document.write(JSON.stringify(entity));
```

Basically we map our fields to Datastore properties and build the `rowRanks` array thanks to the `forEach()` loop. 

Now we modify it slightly to run in the template code instead of a browser:

```js
function transform(elem) {
  var data = JSON.parse(elem);
  ...
  return JSON.stringify(entity);
}
```

Then, we upload the files to GCS and follow the instructions [here][2] to execute it:

```bash
gcloud dataflow jobs run test-datastore \
--gcs-location=gs://dataflow-templates/latest/GCS_Text_to_Datastore \
--parameters=javascriptTextTransformGcsPath=gs://$BUCKET/*.js,errorWritePath=gs://$BUCKET/errors.txt,javascriptTextTransformFunctionName=transform,textReadPattern=gs://$BUCKET/*.json,datastoreWriteProjectId=$PROJECT
```

the full content of the js file uploaded to GCS is available in `parse_entities.js`.

The job runs successfully:

[![enter image description here][3]][3]

and the data is written to Datastore:

[![enter image description here][4]][4]

As of now the Google-provided template uses Java 2.14.0 SDK


  [1]: https://cloud.google.com/datastore/docs/reference/data/rest/v1/Entity
  [2]: https://cloud.google.com/dataflow/docs/guides/templates/provided-batch#gcstexttodatastore
  [3]: https://i.stack.imgur.com/2xfb4.png
  [4]: https://i.stack.imgur.com/qetPn.png

## License

These examples are provided under the Apache License 2.0.

## Issues

Report any issue to the GitHub issue tracker.
