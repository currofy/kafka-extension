package com.github.fmcejudo.kafka.extensions.serialization;


import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Map;

public class OpenTracingSerializer implements Serializer<Trace> {

    private final StringSerializer stringSerializer;

    private final ObjectMapper objectMapper;

    public OpenTracingSerializer() {
        this(new ObjectMapper());
    }

    public OpenTracingSerializer(final ObjectMapper objectMapper) {
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
        byte[] bytes = objectMapper.writeValueAsBytes(s);
        return stringSerializer.serialize(s, new String(bytes));
    }

    @Override
    public void close() {
        stringSerializer.close();
    }
}
