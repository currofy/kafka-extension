package com.currofy.kafka.extensions.opentracing.serialization;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import zipkin2.Span;

import java.util.Map;

public class SpanSerde implements Serde<Span> {

    private final SpanSerializer spanSerializer;
    private final SpanDeserializer spanDeserializer;

    public SpanSerde() {
        this(new SpanSerializer(), new SpanDeserializer());
    }

    public SpanSerde(final SpanSerializer spanSerializer,
                     final SpanDeserializer spanDeserializer) {
        this.spanSerializer = spanSerializer;
        this.spanDeserializer = spanDeserializer;
    }

    @Override
    public Serializer<Span> serializer() {
        return spanSerializer;
    }

    @Override
    public Deserializer<Span> deserializer() {
        return spanDeserializer;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        this.spanSerializer.configure(configs, isKey);
        this.spanDeserializer.configure(configs, isKey);
    }

    @Override
    public void close() {
        this.spanSerializer.close();
        this.spanDeserializer.close();
    }
}
