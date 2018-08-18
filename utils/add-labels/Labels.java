import java.util.HashMap;
import java.util.Map;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
...

public class Labels {
    public static interface MyOptions extends DataflowPipelineOptions {
		...
    }

    public static void main(String[] args) {
            MyOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(MyOptions.class);

            Map<String, String> labels = new HashMap<String, String>();
            labels.put("key1", "value1");
            labels.put("key2", "value2");
            labels.put("key3", "value3");
            options.setLabels(labels);

            Pipeline p = Pipeline.create(options);
            ...
    }
}