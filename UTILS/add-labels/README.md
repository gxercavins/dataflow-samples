## Add Labels

How to set Labels to a Dataflow job from within the code using [`setLabels`](https://beam.apache.org/documentation/sdks/javadoc/2.6.0/index.html?org/apache/beam/runners/dataflow/options/DataflowPipelineOptions.html). Labels have to be provided as key-value pairs such as:

``` java
Map<String, String> labels = new HashMap<String, String>();
labels.put("key1", "value1");
labels.put("key2", "value2");
labels.put("key3", "value3");
options.setLabels(labels);
```

![labels](https://user-images.githubusercontent.com/29493411/44301850-6f82f580-a31e-11e8-80ab-e30fa7452c3f.png)

## License

These examples are provided under the Apache License 2.0.

## Issues

Report any issue to the GitHub issue tracker.
