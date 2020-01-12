package com.github.fmcejudo.kafka.extensions.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class OpentracingSerde implements Serde<Trace> {

    private final OpenTracingSerializer openTracingSerializer;
    private final OpenTracingDeserializer openTracingDeserializer;
    private final ObjectMapper objectMapper;

    public OpentracingSerde() {
        this.objectMapper = new ObjectMapper();
        this.openTracingDeserializer = new OpenTracingDeserializer(objectMapper);
        this.openTracingSerializer = new OpenTracingSerializer(objectMapper);
    }

    @Override
    public Serializer<Trace> serializer() {
        return openTracingSerializer;
    }

    @Override
    public Deserializer<Trace> deserializer() {
        return openTracingDeserializer;
    }


    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        this.openTracingSerializer.configure(configs, isKey);
        this.openTracingDeserializer.configure(configs, isKey);
    }

    @Override
    public void close() {
        this.openTracingSerializer.close();
        this.openTracingDeserializer.close();
    }
}
