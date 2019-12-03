package com.dataflow.samples;

import java.util.Arrays;
import java.util.List;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AssignSessions {

    private static final Logger LOG = LoggerFactory.getLogger(AssignSessions.class);

	@DefaultCoder(AvroCoder.class)
	public static class Session {
	    String id;
	    long start; 
	    long stop;

	    public Session(String id, long start, long stop) {
			this.id = id;
			this.start = start;
			this.stop = stop;
		}

        public Session() {
            // for serialization only
        }
	}

	@DefaultCoder(AvroCoder.class)
	public static class Event {
		String id;
	    long timestamp;

	    public Event(String id, long timestamp) {
			this.id = id;
			this.timestamp = timestamp;
		}

        public Event() {
            // for serialization only
        }
	}

    public static class AssignFn extends DoFn<Event, KV<Session, Event>> {  

        final PCollectionView<List<Session>> sessionPC;
        
        public AssignFn(PCollectionView<List<Session>> TagsideInput) {
            this.sessionPC = TagsideInput;
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            Event event = c.element();

            // get side input with all possible Sessions
            List<Session> sessions = c.sideInput(sessionPC);

            // where does the Event fall in?
            for (Session session:sessions) { 
                if (event.timestamp >= session.start && event.timestamp <= session.stop) {
                    c.output(KV.of(session, event));
                    break;
                }
            }
        }
    }

    public static class LogFn extends DoFn<KV<Session, Iterable<Event>>, KV<Session, Iterable<Event>>> {  

        @ProcessElement
        public void processElement(ProcessContext c) {
            Session session = c.element().getKey();
            Iterable<Event> events = c.element().getValue();
            StringBuilder str = new StringBuilder(); 
          
            // print session info
            str.append(String.format("\nSession id=%s, start=%d, stop=%d", session.id, session.start, session.stop));

            // print each event info
            for (Event event:events) { 
                str.append(String.format("\n---Event id=%s, timestamp=%d", event.id, event.timestamp));
            }

            LOG.info(str.toString());

            c.output(c.element());
        }
    }

    public static void main(String[] args) {

        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(PipelineOptions.class);

        Pipeline p = Pipeline.create(options);

        // Example sessions data
        final List<Session> sessionList = Arrays.asList(
            new Session("s1", 0L, 100L),
            new Session("s2", 100L, 200L),
            new Session("s3", 200L, 300L)
        );

        // Example event data
        final List<Event> eventList = Arrays.asList(
            new Event("e1", 20L),
            new Event("e2", 60L),
            new Event("e3", 120L),
            new Event("e4", 160L),
            new Event("e5", 210L),
            new Event("e6", 290L)            
        );

        // create PCollectionView from sessions
        final PCollectionView<List<Session>> sessionPC = p
            .apply("Create Sessions", Create.of(sessionList))
            .apply("Save as List", View.asList());

		p
			.apply("Create Events", Create.of(eventList))
		    .apply("Assign Sessions", ParDo.of(new AssignFn(sessionPC))
                .withSideInputs(sessionPC))
            .apply("Group By Key", GroupByKey.<Session,Event>create())
            .apply("Log Grouped Results", ParDo.of(new LogFn()));

        p.run().waitUntilFinish();
    }
}
