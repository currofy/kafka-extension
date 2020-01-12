package com.github.fmcejudo.kafka.extensions.serialization;


import lombok.SneakyThrows;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class OpenTracingDeserializerTest {

    private final String sampleTopic = "mytopic";

    ClassLoader classLoader;

    OpenTracingDeserializer openTracingDeserializer;


    @BeforeEach
    void setUp() {
        this.classLoader = getClass().getClassLoader();
        this.openTracingDeserializer = new OpenTracingDeserializer();
    }

    @Test
    @SneakyThrows
    void shouldDeserializeAOpenTracingStringIntoASpan() {
        //Given
        InputStream inputStream = Optional.ofNullable(classLoader.getResourceAsStream("span.json"))
                .orElseThrow(() -> new RuntimeException("It could not read resource to load span"));

        byte[] spanBytes = IOUtils.toByteArray(inputStream);

        //When
        Trace trace = openTracingDeserializer.deserialize(sampleTopic, spanBytes);

        //Then
        assertThat(trace.getTraceId()).isNotNull().isEqualTo("d566efbd4dc4a74f");
        assertThat(trace.getSpans()).isNotNull().hasSize(2);
    }
}