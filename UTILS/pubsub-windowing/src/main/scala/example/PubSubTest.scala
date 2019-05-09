package example

import com.spotify.scio._
import com.spotify.scio.values.{SCollection, WindowOptions}
import org.apache.beam.sdk.{Pipeline, PipelineResult}
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.transforms.windowing._
import org.apache.beam.sdk.transforms.windowing.{AfterPane, Repeatedly}
import org.apache.beam.sdk.transforms.windowing.Window.ClosingBehavior
import org.apache.beam.sdk.values._
import org.apache.beam.sdk.values.WindowingStrategy.AccumulationMode
import org.joda.time.Duration
import org.slf4j.LoggerFactory

/*
sbt "runMain example.PubSubTest
  --project=[PROJECT] --runner=DataflowRunner --zone=[ZONE]
  --input=[SUBSCRIPTION_NAME]"
*/

object PubSubTest {
  private lazy val log = LoggerFactory.getLogger(this.getClass)

  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    val defaultInputSub = "test_sub"
    val subscription = args.getOrElse("input", defaultInputSub)
    val project = args.get("project")

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