package com.github.fmcejudo.kafka.extensions.opentracing.serialization;

import lombok.SneakyThrows;
import org.apache.commons.io.IOUtils;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import zipkin2.Span;

import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static zipkin2.Span.Kind.CLIENT;

class SpanDeserializerTest {

    ClassLoader classLoader;

    SpanDeserializer spanDeserializer;


    @BeforeEach
    void setUp() {
        this.classLoader = getClass().getClassLoader();
        this.spanDeserializer = new SpanDeserializer();
    }

    @Test
    @SneakyThrows
    void shouldDeserializeASpanStringIntoASpan() {
        //Given
        String sampleTopic = "mytopic";
        InputStream inputStream = Optional.ofNullable(classLoader.getResourceAsStream("isolatedSpan.json"))
                .orElseThrow(() -> new RuntimeException("It could not read resource to load span"));

        byte[] spanBytes = IOUtils.toByteArray(inputStream);

        //When
        Span span = spanDeserializer.deserialize(sampleTopic, spanBytes);

        //Then
        assertThat(span).isNotNull().extracting("kind", "traceId", "parentId", "id", "localServiceName")
                .containsExactly(CLIENT, "46876d22cf0f94e0", "46876d22cf0f94e0", "b8d201046fe8a637", "zipkin-sample");

        //When
        RecordHeader[] headerArray = new RecordHeader[]{new RecordHeader("key", "value".getBytes())};
        RecordHeaders headers = new RecordHeaders(headerArray);
        Span spanWithHeaders = spanDeserializer.deserialize(sampleTopic, headers, spanBytes);

        //Then
        assertThat(span).isNotNull().extracting("kind", "traceId", "parentId", "id", "localServiceName")
                .containsExactly(CLIENT, "46876d22cf0f94e0", "46876d22cf0f94e0", "b8d201046fe8a637", "zipkin-sample");

    }

    @Test
    void shouldDeserializeASpanWithHeadersIntoASpan() throws IOException {

        //Given
        String sampleTopic = "mytopic";
        InputStream inputStream = Optional.ofNullable(classLoader.getResourceAsStream("isolatedSpan.json"))
                .orElseThrow(() -> new RuntimeException("It could not read resource to load span"));

        byte[] spanBytes = IOUtils.toByteArray(inputStream);

        //When
        RecordHeader[] headerArray = new RecordHeader[]{new RecordHeader("key", "value".getBytes())};
        RecordHeaders headers = new RecordHeaders(headerArray);
        Span spanWithHeaders = spanDeserializer.deserialize(sampleTopic, headers, spanBytes);

        //Then
        assertThat(spanWithHeaders).isNotNull().extracting("kind", "traceId", "parentId", "id", "localServiceName")
                .containsExactly(CLIENT, "46876d22cf0f94e0", "46876d22cf0f94e0", "b8d201046fe8a637", "zipkin-sample");

    }
}