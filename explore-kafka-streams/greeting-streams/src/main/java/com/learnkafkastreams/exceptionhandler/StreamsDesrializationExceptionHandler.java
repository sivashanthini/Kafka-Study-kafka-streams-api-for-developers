package com.learnkafkastreams.exceptionhandler;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.util.Map;

@Slf4j
public class StreamsDesrializationExceptionHandler implements DeserializationExceptionHandler {
    int errorCount  = 2;
    @Override
    public DeserializationHandlerResponse handle(ProcessorContext processorContext, ConsumerRecord<byte[], byte[]> consumerRecord, Exception e) {
        log.error("Exception : {}, and kafka record is : {}", e.getMessage(), consumerRecord);
        log.info("Error Count : {}", errorCount);
        if (errorCount < 2)
            return DeserializationHandlerResponse.CONTINUE;
        return DeserializationHandlerResponse.FAIL;
    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
