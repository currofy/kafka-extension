package com.github.fmcejudo.kafka.extensions.group.serialization;

import com.github.fmcejudo.kafka.extensions.group.LinkedSpan;
import com.github.fmcejudo.kafka.extensions.group.LinkedSpanList;
import com.github.fmcejudo.kafka.extensions.opentracing.test.NodeTraceGenerator;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import zipkin2.Span;

import static java.nio.charset.StandardCharsets.UTF_8;

@Slf4j
class LinkedSpanListSerializerTest {

    LinkedSpanListSerializer linkedSpanListSerializer;

    @BeforeEach
    void setUp() {
        this.linkedSpanListSerializer = new LinkedSpanListSerializer();
    }

    @Test
    void shouldSerializeALinkedListSpan() {
        //Given
        Span rootSpan = NodeTraceGenerator.generator().generateRootSpan("servicea");
        LinkedSpan linkedSpan = LinkedSpan.of(rootSpan);

        LinkedSpanList linkedSpanList = new LinkedSpanList().include(linkedSpan);

        //When
        byte[] jsonObject = linkedSpanListSerializer.serialize("topic", linkedSpanList);

        //Then
        System.out.println(new String(jsonObject, UTF_8));

    }
}