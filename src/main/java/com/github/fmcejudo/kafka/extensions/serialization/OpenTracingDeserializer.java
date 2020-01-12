package com.github.fmcejudo.kafka.extensions.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import zipkin2.Span;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class OpenTracingDeserializer implements Deserializer<Trace> {

    private final StringDeserializer stringDeserializer;

    private final ObjectMapper objectMapper;

    public OpenTracingDeserializer() {
        this(new ObjectMapper());
    }

    public OpenTracingDeserializer(final ObjectMapper objectMapper) {
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
        Map<String, Object>[] spanArray =
                objectMapper.readValue(stringDeserializer.deserialize(topic, data), Map[].class);

        List<Span> spanList = Stream.of(spanArray)
                .map(this::extractSpanFromMap)
                .collect(Collectors.toList());
        List<String> traceIds = spanList.stream().map(Span::traceId).distinct().collect(Collectors.toList());
        if (traceIds.size() != 1) {
            throw new RuntimeException("Only an unique traceId is allowed in a trace");
        }
        return Trace.builder().spans(spanList).traceId(traceIds.get(0)).build();
    }

    private Span extractSpanFromMap(final Map<String, Object> spanMap) {
        return Span.newBuilder()
                .traceId(String.valueOf(spanMap.get("traceId")))
                .id(String.valueOf(spanMap.get("id")))
                .kind(Span.Kind.valueOf(String.valueOf(spanMap.get("kind"))))
                .build();
    }

    @Override
    public void close() {
        stringDeserializer.close();
    }
}
