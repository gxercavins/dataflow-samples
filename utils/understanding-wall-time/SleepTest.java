import java.util.concurrent.TimeUnit;

...

public class SleepTest {

    ...

	public static void main(String[] args) {

       ...

		p //
				...
				.apply(ParDo.of(new DoFn<..., ...>() {
					@ProcessElement
					public void processElement(ProcessContext c) throws Exception {
						...
						TimeUnit.MINUTES.sleep(10);
						...
						c.output(...);
					}
				})) //
				...

		p.run();
	}
}
