package com.learnkafkastreams.serdes;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafkastreams.domain.Greeting;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;

@Slf4j
public class GreetingsDeserializer implements Deserializer<Greeting> {
    ObjectMapper objectMapper;

    public GreetingsDeserializer(ObjectMapper objectMapper) {this.objectMapper = objectMapper;}

    @Override
    public Greeting deserialize(String topic, byte[] data) {
        try {
            return objectMapper.readValue(data, Greeting.class);
        } catch (IOException e) {
            log.error("Error while deserializing greeting", e.getMessage(), e);
            throw new RuntimeException(e);
        } catch (Exception e) {
            log.error("Exception in deserializing", e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }
}
