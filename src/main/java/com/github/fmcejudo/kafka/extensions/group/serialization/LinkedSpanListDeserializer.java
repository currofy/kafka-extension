package com.github.fmcejudo.kafka.extensions.group.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.fmcejudo.kafka.extensions.group.LinkedSpanList;
import lombok.SneakyThrows;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Map;

public class LinkedSpanListDeserializer implements Deserializer<LinkedSpanList> {

    private final ObjectMapper objectMapper;

    private final StringDeserializer stringDeserializer;

    public LinkedSpanListDeserializer() {
        this.stringDeserializer = new StringDeserializer();
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        stringDeserializer.configure(configs, isKey);
    }

    @Override
    @SneakyThrows
    public LinkedSpanList deserialize(String topic, byte[] data) {
        String json = stringDeserializer.deserialize(topic, data);
        return objectMapper.readValue(json, LinkedSpanList.class);
    }

    @Override
    public void close() {
        stringDeserializer.close();
    }
}
