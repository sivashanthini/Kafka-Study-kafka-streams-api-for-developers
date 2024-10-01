package com.learnkafkastreams.exceptionhandler;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;

@Slf4j
public class StreamProcessorExceptionHandler implements StreamsUncaughtExceptionHandler {
    @Override
    public StreamThreadExceptionResponse handle(Throwable throwable) {
        log.error("Exception in the application : {} ", throwable.getMessage(), throwable);
        if (throwable instanceof StreamsException) {
            var cause = throwable.getCause();
            if (cause.getMessage().equals("Transient Error")) {
                return StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
            }
        }
        return StreamThreadExceptionResponse.SHUTDOWN_APPLICATION;
    }
}
