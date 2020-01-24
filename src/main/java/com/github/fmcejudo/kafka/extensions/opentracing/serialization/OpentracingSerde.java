package com.github.fmcejudo.kafka.extensions.opentracing.serialization;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import zipkin2.Span;

import java.util.List;
import java.util.Map;

public class OpentracingSerde implements Serde<List<Span>> {

    private final OpentracingSerializer opentracingSerializer;
    private final OpentracingDeserializer opentracingDeserializer;

    public OpentracingSerde() {
        this(new OpentracingSerializer(), new OpentracingDeserializer());
    }

    public OpentracingSerde(final OpentracingSerializer opentracingSerializer,
                            final OpentracingDeserializer opentracingDeserializer) {
        this.opentracingSerializer = opentracingSerializer;
        this.opentracingDeserializer = opentracingDeserializer;
    }

    @Override
    public Serializer<List<Span>> serializer() {
        return opentracingSerializer;
    }

    @Override
    public Deserializer<List<Span>> deserializer() {
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
