package com.learnkafkastreams.serdes;

import com.learnkafkastreams.domain.Order;
import com.learnkafkastreams.domain.Revenue;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

public class SerdeFactory {
    public static Serde<Order> orderSerdeGenerics() {
        JsonSerializer<Order> jsonSerializer = new JsonSerializer<>();
        JsonDeserializer<Order> jsonDeserializer = new JsonDeserializer<>(Order.class);
        return Serdes.serdeFrom(jsonSerializer, jsonDeserializer);
    }

    public static Serde<Revenue> revenueSerdeGenerics() {
        JsonSerializer<Revenue> jsonSerializer = new JsonSerializer<>();
        JsonDeserializer<Revenue> jsonDeserializer = new JsonDeserializer<>(Revenue.class);
        return Serdes.serdeFrom(jsonSerializer, jsonDeserializer);
    }
}
