package com.github.fmcejudo.kafka.extensions.serialization;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import zipkin2.Span;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

class OpenTracingSerializerTest {

    private final String topic = "mytopic";

    OpentracingSerializer opentracingSerializer;

    @BeforeEach
    void setUp() {
        opentracingSerializer = new OpentracingSerializer();
    }

    @Test
    void shouldSerializeASpanIntoOpentracingString() {

        //Given
        String traceId = "0a0a0a0a";
        String spanId = "faf1faf0";

        Span span = Span.newBuilder()
                .traceId(traceId)
                .id(spanId)
                .kind(Span.Kind.SERVER)
                .duration(4583L)
                .name("my_service").build();
        Trace trace = Trace.builder().spans(Collections.singletonList(span)).traceId(spanId).build();

        //When
        byte[] json2ZipkinSpan = opentracingSerializer.serialize(topic, trace);

        //Then
        assertThat(new String(json2ZipkinSpan))
                .contains("traceId").contains(traceId)
                .contains("\"id\"").contains(spanId)
                .contains("kind").contains(Span.Kind.SERVER.name())
                .contains("name").contains("my_service");

    }

}