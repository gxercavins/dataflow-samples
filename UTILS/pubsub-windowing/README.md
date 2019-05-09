## Pub/Sub and windowing

Quick example to address the issues on [this StackOverflow question](https://stackoverflow.com/questions/52334402/scio-apache-beam-how-to-map-grouped-results
) and [issue](https://github.com/spotify/scio/issues/1366). It uses [Spotify's Scio](https://github.com/spotify/scio), which is a Scala API on top of Beam's Java SDK.

## Example

A simple pipeline that reads from a Pub/Sub subscription, uses the first word of the message as the key, applies the `GroupByKey` and then logs the entries. To work with the DataflowRunner, for those versions, it is necessary to add the triggering strategy. Otherwose, the GBK step waits for all the data to arrive and, as it's an unbounded source, does not emit any result. Main code:

```scala
object PubSubTest {
  private lazy val log = LoggerFactory.getLogger(this.getClass)

  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    val defaultInputSub = "test_sub"
    val subscription = args.getOrElse("input", defaultInputSub)
    val project = "PROJECT_ID"

    sc.pubsubSubscription[String](s"projects/$project/subscriptions/$subscription")
      // provide window options including triggering
      .withFixedWindows(duration = Duration.standardSeconds(10), options = WindowOptions(
        trigger = Repeatedly.forever(AfterProcessingTime.pastFirstElementInPane()
          .plusDelayOf(Duration.standardSeconds(2))),
        accumulationMode = AccumulationMode.ACCUMULATING_FIRED_PANES,
        closingBehavior = ClosingBehavior.FIRE_IF_NON_EMPTY,
        allowedLateness = Duration.standardSeconds(0))
      )
      // use first word of the Pub/Sub message as the key
      .keyBy(a => a.split(" ")(0))
      .groupByKey
      .map(entry => log.info(s"${entry._1} was repeated ${entry._2.size} times"))

    val result = sc.close().waitUntilFinish()
  }
}
```

Tested with Scio 0.6.1 and Beam 2.6.0.

## License

These examples are provided under the Apache License 2.0.

## Issues

Report any issue to the GitHub issue tracker.
