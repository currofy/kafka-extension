package com.github.fmcejudo.kafka.extensions.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import zipkin2.Span;
import zipkin2.SpanBytesDecoderDetector;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class OpentracingDeserializer implements Deserializer<Trace> {

    private final StringDeserializer stringDeserializer;

    private final ObjectMapper objectMapper;

    public OpentracingDeserializer() {
        this(new ObjectMapper());
    }

    public OpentracingDeserializer(final ObjectMapper objectMapper) {
        this.stringDeserializer = new StringDeserializer();
        this.objectMapper = objectMapper;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        stringDeserializer.configure(configs, isKey);
    }

    @Override
    @SneakyThrows
    public Trace deserialize(String topic, byte[] data) {
        List<Span> spanList = decodeSpans(data);
        List<String> traceIds = spanList.stream().map(Span::traceId).distinct().collect(Collectors.toList());
        if (traceIds.size() != 1) {
            throw new RuntimeException("Only an unique traceId is allowed in a trace");
        }
        return Trace.builder().spans(spanList).traceId(traceIds.get(0)).build();
    }

    private List<Span> decodeSpans(final byte[] data) {
        return SpanBytesDecoderDetector.decoderForListMessage(data).decodeList(data);
    }

    @Override
    public void close() {
        stringDeserializer.close();
    }
}
