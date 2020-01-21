package com.github.fmcejudo.kafka.extensions.opentracing.serialization;

import com.github.fmcejudo.kafka.extensions.opentracing.NodeTrace;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class OpentracingSerde implements Serde<NodeTrace> {

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
    public Serializer<NodeTrace> serializer() {
        return opentracingSerializer;
    }

    @Override
    public Deserializer<NodeTrace> deserializer() {
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
