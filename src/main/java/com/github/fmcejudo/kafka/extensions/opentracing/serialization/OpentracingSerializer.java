package com.github.fmcejudo.kafka.extensions.opentracing.serialization;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.fmcejudo.kafka.extensions.opentracing.Trace;
import lombok.SneakyThrows;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import zipkin2.codec.SpanBytesEncoder;

import java.util.Map;

public class OpentracingSerializer implements Serializer<Trace> {


    //TODO(cejudogomezf) This should find a configuration property which select the type of encoder:
    // JSON_V1, JSON_V2,PROTO3, THRIFT

    private final StringSerializer stringSerializer;

    private final ObjectMapper objectMapper;

    public OpentracingSerializer() {
        this(new ObjectMapper());
    }

    public OpentracingSerializer(final ObjectMapper objectMapper) {
        this.stringSerializer = new StringSerializer();
        this.objectMapper = objectMapper;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        stringSerializer.configure(configs, isKey);
    }

    @Override
    @SneakyThrows
    public byte[] serialize(String s, Trace trace) {
        return SpanBytesEncoder.JSON_V2.encodeList(trace.getSpans());
    }

    @Override
    public void close() {
        stringSerializer.close();
    }
}
