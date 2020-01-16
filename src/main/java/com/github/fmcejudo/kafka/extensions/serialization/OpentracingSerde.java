package com.github.fmcejudo.kafka.extensions.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class OpentracingSerde implements Serde<Trace> {

    private final OpentracingSerializer opentracingSerializer;
    private final OpentracingDeserializer opentracingDeserializer;
    private final ObjectMapper objectMapper;

    public OpentracingSerde() {
        this.objectMapper = new ObjectMapper();
        this.opentracingDeserializer = new OpentracingDeserializer(objectMapper);
        this.opentracingSerializer = new OpentracingSerializer(objectMapper);
    }

    @Override
    public Serializer<Trace> serializer() {
        return opentracingSerializer;
    }

    @Override
    public Deserializer<Trace> deserializer() {
        return opentracingDeserializer;
    }


    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        this.opentracingSerializer.configure(configs, isKey);
        this.opentracingDeserializer.configure(configs, isKey);
    }

    @Override
    public void close() {
        this.opentracingSerializer.close();
        this.opentracingDeserializer.close();
    }
}
