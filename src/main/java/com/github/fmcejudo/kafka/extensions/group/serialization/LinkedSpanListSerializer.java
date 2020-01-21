package com.github.fmcejudo.kafka.extensions.group.serialization;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.fmcejudo.kafka.extensions.group.LinkedSpanList;
import lombok.SneakyThrows;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

import static java.nio.charset.StandardCharsets.UTF_8;

public class LinkedSpanListSerializer implements Serializer<LinkedSpanList> {

    private ObjectMapper objectMapper;

    public LinkedSpanListSerializer() {
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    @SneakyThrows
    public byte[] serialize(String s, LinkedSpanList linkedSpanList) {
        return objectMapper.writeValueAsBytes(linkedSpanList);
    }

    @Override
    public void close() {

    }
}
