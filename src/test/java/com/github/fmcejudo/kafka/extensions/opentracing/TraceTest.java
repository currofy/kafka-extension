package com.github.fmcejudo.kafka.extensions.opentracing;

import com.github.fmcejudo.kafka.extensions.opentracing.serialization.OpentracingDeserializer;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class TraceTest {

    private ClassLoader classLoader;
    private OpentracingDeserializer opentracingDeserializer;

    @BeforeEach
    void setUp() {
        classLoader = this.getClass().getClassLoader();
        opentracingDeserializer = new OpentracingDeserializer();
    }

    @Test
    void shouldDescribeSpans() throws IOException {
        //Given
        InputStream inputStream = Optional.ofNullable(classLoader.getResourceAsStream("span.json"))
                .orElseThrow(() -> new RuntimeException("It could not read resource to load span"));

        byte[] spanBytes = IOUtils.toByteArray(inputStream);

        //When
        Trace trace = opentracingDeserializer.deserialize(null, spanBytes);

        //Then
        assertThat(trace.containsRoot()).isTrue();
        assertThat(trace.serverSpans()).hasSize(1);
        assertThat(trace.duration()).isEqualTo(369236L);
        assertThat(trace.serviceNames()).contains("zipkin-sample");
    }

}