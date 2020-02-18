package com.currofy.kafka.extensions.opentracing.serialization;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import zipkin2.Span;
import zipkin2.codec.SpanBytesDecoder;

class SpanDeserializer implements Deserializer<Span> {

    @Override
    public Span deserialize(String topic, byte[] data) {
        return SpanBytesDecoder.JSON_V2.decodeOne(data);
    }

    @Override
    public Span deserialize(String topic, Headers headers, byte[] data) {
        return deserialize(topic, data);
    }

}
