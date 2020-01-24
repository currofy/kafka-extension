package com.github.fmcejudo.kafka.extensions.opentracing.serialization;


import lombok.SneakyThrows;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import zipkin2.Span;

import java.io.InputStream;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class OpentracingDeserializerTest {

    private final String sampleTopic = "mytopic";

    ClassLoader classLoader;

    OpentracingDeserializer openTracingDeserializer;


    @BeforeEach
    void setUp() {
        this.classLoader = getClass().getClassLoader();
        this.openTracingDeserializer = new OpentracingDeserializer();
    }

    @Test
    @SneakyThrows
    void shouldDeserializeAOpenTracingStringIntoASpan() {
        //Given
        InputStream inputStream = Optional.ofNullable(classLoader.getResourceAsStream("span-zipkin-sample.json"))
                .orElseThrow(() -> new RuntimeException("It could not read resource to load span"));

        byte[] spanBytes = IOUtils.toByteArray(inputStream);

        //When
        List<Span> spans = openTracingDeserializer.deserialize(sampleTopic, spanBytes);

        //Then
        assertThat(spans).isNotNull().hasSize(2);
    }
}