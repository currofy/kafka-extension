package com.github.fmcejudo.kafka.extensions.opentracing.serialization;


import lombok.SneakyThrows;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import zipkin2.Span;
import zipkin2.codec.SpanBytesEncoder;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

class TraceSerializer implements Serializer<List<Span>> {

    private static final Map<String, SpanBytesEncoder> BYTES_ENCODER_MAP;

    static {
        BYTES_ENCODER_MAP = new HashMap<>();
        BYTES_ENCODER_MAP.put("json_v1", SpanBytesEncoder.JSON_V1);
        BYTES_ENCODER_MAP.put("json_v2", SpanBytesEncoder.JSON_V2);
        BYTES_ENCODER_MAP.put("thrift", SpanBytesEncoder.THRIFT);
        BYTES_ENCODER_MAP.put("proto3", SpanBytesEncoder.PROTO3);
    }

    private final StringSerializer stringSerializer;

    //TODO (cejudogomezf) this property should be private, but at the moment it is like this not to forget testing it.
    SpanBytesEncoder bytesEncoder;

    {
        bytesEncoder = SpanBytesEncoder.JSON_V2;
    }

    public TraceSerializer() {
        this.stringSerializer = new StringSerializer();
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        if (configs.containsKey("span.encoder")) {
            String encoder = (String) configs.get("span.encoder");
            bytesEncoder = BYTES_ENCODER_MAP.getOrDefault(encoder, SpanBytesEncoder.JSON_V2);
        }
        stringSerializer.configure(configs, isKey);
    }

    @Override
    @SneakyThrows
    public byte[] serialize(String s, List<Span> spans) {
        return bytesEncoder.encodeList(spans);
    }

    @Override
    public void close() {
        stringSerializer.close();
    }
}
