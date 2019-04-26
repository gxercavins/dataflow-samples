## Throttle a step

Dataflow is able to scale up as needed to dispatch elements but sometimes we might be interested in throttling a particular step. For example, to avoid hitting rate quotas or sending requests to a back-end that we know that can't process files that fast. The `ThrottleStep.java` file adapts the typical WordCount example to demonstrate one possible approach, written as an answer to StackOverflow [question](https://stackoverflow.com/questions/52183538/throttling-a-step-in-beam-application/).

One of the simplest solutions is to introduce a sleep in the step. For this, we need to know the maximum number of instances of the ParDo that can be running at the same time. If `autoscalingAlgorithm` is set to `NONE` (i.e. constant number of workers) we can calculate that from `numWorkers` and `workerMachineType` (DataflowPipelineOptions). Precisely, the (minimum) effective rate will be the desired rate divided by the total number of threads: `desired_rate/(num_workers*num_threads(per worker))`. The worst-case sleep time will be the inverse of the previous effective rate:

```java
Integer num_workers, num_threads;
String machine_type;

Integer desired_rate = 1; // in the example we do not want to exceed 1 request per second

if (options.getNumWorkers() == 0) { num_workers = 1; }
else { num_workers = options.getNumWorkers(); }

// catch NPE
if (options.getWorkerMachineType() != null) { 
    machine_type = options.getWorkerMachineType();
    num_threads = Integer.parseInt(machine_type.substring(machine_type.length() - 1));
}
else { num_threads = 1; }

// we need to account for the max number of DoFn instances executing simultaneously
Double sleep_time = (double)(num_workers * num_threads) / (double)(desired_rate);
```

Then, we can use `TimeUnit.SECONDS.sleep(sleep_time.intValue());` or equivalent inside the throttled Fn. In my example, as a use case, I wanted to read from a public file, parse out the empty lines and call the Natural Language Processing API with a maximum rate of 1 QPS (I initialized `desired_rate` to 1 previously):

```java
.apply("NLP requests", ParDo.of(new DoFn<String, String> () {
    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
        // Instantiates a client
        try (LanguageServiceClient language = LanguageServiceClient.create()) {

          // The text to analyze
          String text = c.element();
          Document doc = Document.newBuilder()
              .setContent(text).setType(Type.PLAIN_TEXT).build();

          // Detects the sentiment of the text
          Sentiment sentiment = language.analyzeSentiment(doc).getDocumentSentiment();                 
          String nlp_results = String.format("Sentiment: score %s, magnitude %s", sentiment.getScore(), sentiment.getMagnitude());

          TimeUnit.SECONDS.sleep(sleep_time.intValue()); // i.e 12s for 3 workers, n1-standard-4

          Log.info(nlp_results);
          c.output(nlp_results);
        }
    }
```

With this, I get a 1 element/s rate as seen in the image below and avoid hitting quota when using multiple workers, even if requests are not really spread out (you might get 8 simultaneous requests and then 8s sleep for each, etc.). This was just a test, possibly a better implemention would be using guava's [rateLimiter][1].

[![enter image description here][2]][2]

If the pipeline is using autoscaling (`THROUGHPUT_BASED`) then it would be more complicated and the number of workers should be updated (for example, Stackdriver Monitoring has a [`job/current_num_vcpus`](https://cloud.google.com/monitoring/api/metrics_gcp#gcp-dataflow) metric). Other general considerations would be controlling the number of parallel ParDos by using a dummy GroupByKey or splitting the source with splitIntoBundles, etc.

Note that I found some dependency issues with the NLP library and 2.5.0 Java SDK so I had to add the following to `pom.xml`:

```xml
<dependency>
    <groupId>com.google.cloud</groupId>
    <artifactId>google-cloud-language</artifactId>
    <version>0.17.0-beta</version>
        <exclusions>
          <exclusion>
            <groupId>com.google.api.grpc</groupId>
            <artifactId>grpc-google-cloud-language-v1</artifactId>
          </exclusion>
        </exclusions>
</dependency>
```

  [1]: https://google.github.io/guava/releases/19.0/api/docs/index.html?com/google/common/util/concurrent/RateLimiter.html
  [2]: https://i.stack.imgur.com/xenBh.png

## License

These examples are provided under the Apache License 2.0.

## Issues

Report any issue to the GitHub issue tracker.
