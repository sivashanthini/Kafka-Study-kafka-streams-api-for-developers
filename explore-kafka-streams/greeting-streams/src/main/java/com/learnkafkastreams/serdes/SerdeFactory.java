package com.learnkafkastreams.serdes;

import com.learnkafkastreams.domain.Greeting;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

public class SerdeFactory {
    public static GreetingSerde greetingSerde() {
        return new GreetingSerde();
    }
    public static Serde<Greeting> greetingSerdeGenerics() {
        JsonSerializer<Greeting> jsonSerializer = new JsonSerializer<>();
        JsonDeserializer<Greeting> jsonDeserializer = new JsonDeserializer<>(Greeting.class);
        return Serdes.serdeFrom(jsonSerializer, jsonDeserializer);
    }
}
