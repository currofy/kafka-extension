package com.github.fmcejudo.kafka.extensions.opentracing.serialization;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import zipkin2.Span;
import zipkin2.codec.SpanBytesDecoder;

import java.util.Map;

public class SpanDeserializer implements Deserializer<Span> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public Span deserialize(String topic, byte[] data) {
        return SpanBytesDecoder.JSON_V2.decodeOne(data);
    }

    @Override
    public Span deserialize(String topic, Headers headers, byte[] data) {
        return deserialize(topic, data);
    }

    @Override
    public void close() {

    }
}
