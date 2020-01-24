package com.github.fmcejudo.kafka.extensions.group.serialization;

import com.github.fmcejudo.kafka.extensions.group.LinkedSpanList;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class LinkedSpanListSerde implements Serde<LinkedSpanList> {

    private final LinkedSpanListSerializer linkedSpanListSerializer;
    private final LinkedSpanListDeserializer linkedSpanListDeserializer;

    public LinkedSpanListSerde() {
        this(new LinkedSpanListSerializer(), new LinkedSpanListDeserializer());
    }

    public LinkedSpanListSerde(final LinkedSpanListSerializer linkedSpanListSerializer,
                               final LinkedSpanListDeserializer linkedSpanListDeserializer) {
        this.linkedSpanListSerializer = linkedSpanListSerializer;
        this.linkedSpanListDeserializer = linkedSpanListDeserializer;
    }

    @Override
    public Serializer<LinkedSpanList> serializer() {
        return linkedSpanListSerializer;
    }

    @Override
    public Deserializer<LinkedSpanList> deserializer() {
        return linkedSpanListDeserializer;
    }


    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        this.linkedSpanListSerializer.configure(configs, isKey);
        this.linkedSpanListDeserializer.configure(configs, isKey);
    }

    @Override
    public void close() {
        this.linkedSpanListSerializer.close();
        this.linkedSpanListDeserializer.close();
    }
}
