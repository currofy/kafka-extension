package com.github.fmcejudo.kafka.extensions.opentracing.serialization;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;
import zipkin2.Span;
import zipkin2.codec.SpanBytesEncoder;

class SpanSerializer implements Serializer<Span> {

    @Override
    public byte[] serialize(String topic, Span data) {
        return SpanBytesEncoder.JSON_V2.encode(data);
    }

    @Override
    public byte[] serialize(String topic, Headers headers, Span data) {
        return serialize(topic, data);
    }

}
