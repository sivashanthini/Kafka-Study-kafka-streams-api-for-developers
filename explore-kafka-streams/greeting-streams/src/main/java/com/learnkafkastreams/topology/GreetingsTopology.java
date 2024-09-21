package com.learnkafkastreams.topology;

import com.learnkafkastreams.domain.Greeting;
import com.learnkafkastreams.serdes.SerdeFactory;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;

public class GreetingsTopology {
    public static final String GREETINGS = "greetings";
    public static final String GREETINGS_UPPERCASE = "greetings_uppercase";
    public static final String GREETINGS_SPANISH = "greetings_spanish";
    public static Topology buildTopology() {
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        var mergeStream = getCustomGreetingKStream(streamsBuilder);
        mergeStream.print(Printed.<String, Greeting>toSysOut().withLabel("Merged Stream"));

        var greetingsProcessorStream = mergeStream.mapValues(value -> {
            return new Greeting(value.message().toUpperCase(), value.timeStamp());
        });
        greetingsProcessorStream.print(Printed.<String, Greeting>toSysOut().withLabel("Processor Stream"));

        greetingsProcessorStream.to(GREETINGS_UPPERCASE, Produced.with(Serdes.String(), SerdeFactory.greetingSerde()));
        return streamsBuilder.build();
    }

    private static KStream<String, Greeting> getCustomGreetingKStream(StreamsBuilder streamsBuilder) {
        var greetingsSourceStream = streamsBuilder.stream(GREETINGS, Consumed.with(Serdes.String(), SerdeFactory.greetingSerde()));
        var greetingsSpanishSourceStream = streamsBuilder.stream(GREETINGS_SPANISH, Consumed.with(Serdes.String(), SerdeFactory.greetingSerde()));
        var mergeStream = greetingsSourceStream.merge(greetingsSpanishSourceStream);
        return mergeStream;
    }
}
