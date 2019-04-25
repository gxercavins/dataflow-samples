## Understanding wall time

When inspecting a Cloud Dataflow pipeline through the UI you can click any step to bring up a sidebar containing the step summary. One of the metrics shown is the `wall time`. The question mark icon next to it will pop up the following message on hover:

> Approximate time spent in this step on initializing, processing data, shuffling data, and terminating, across all threads in all workers. For composite steps, the sum of time spent in the component steps. This estimate helps you identify slow steps.

The main idea is to help optimize the pipeline and identify possible bottlenecks, where most of the time the SDK spends compute time. So far so good but a common question is:

> I just started my job a few minutes ago and the wall time for a certain step is already up to a few hours.

This does not necessarily indicate that there is something wrong. As the description says, this is aggregated across all workers working in parallel and all the corresponding threads. As a quick way to test this I ran three simultaneous jobs that read from three different Pub/Sub subscriptions and publish 1, 5 and 10 messages to each one, respectively (using `publish.sh`).

In one of the steps I added a 10-minute sleep to easily visualize wall time with `TimeUnit.MINUTES.sleep()`:

``` java
.apply(ParDo.of(new DoFn<..., ...>() {
	@ProcessElement
	public void processElement(ProcessContext c) throws Exception {
		...
		TimeUnit.MINUTES.sleep(10);
		...
		c.output(...);
	}
})) //
```

For these streaming jobs I used a [`n1-standard-4`](https://cloud.google.com/compute/docs/machine-types#standard_machine_types) machine type which has 4 vCPUs/allocated threads. Therefore, for example, the job that had to read 5 messages read 4 of them initially and then all threads were stuck on the sleep operation. The final message was not read until one of the previous elements was processed and the thread freed.

Finally, as shown in the image below, total wall time was a multiple of the number of elements that the pipeline processed:

* 1 element - 10 min
* 5 elements - 50 min
* 10 elements - 100 min (1h40 min)

![wall_time](https://user-images.githubusercontent.com/29493411/44301678-6cd2d100-a31b-11e8-9657-fbdc33623cbd.png)

## License

These examples are provided under the Apache License 2.0.

## Issues

Report any issue to the GitHub issue tracker.
