package com.github.fmcejudo.kafka.extensions.opentracing;

import com.github.fmcejudo.kafka.extensions.opentracing.serialization.OpentracingDeserializer;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static zipkin2.Span.Kind.SERVER;

class AggregatedNodeTraceTest {

    OpentracingDeserializer opentracingDeserializer;

    ClassLoader classLoader;

    @BeforeEach
    void setUp() {
        this.opentracingDeserializer = new OpentracingDeserializer();
        this.classLoader = this.getClass().getClassLoader();
    }

    @Test
    void shouldMergeTwoTracesFromSameRequest() {
        //Given
        NodeTrace sampleZipkin = readTraceFromFile("span-zipkin-sample.json");
        NodeTrace sampleWeather = readTraceFromFile("span-zipkin-weather-sample.json");

        //When
        AggregatedTrace aggregatedTrace = AggregatedTrace.withInitial(sampleZipkin, NodeTrace::getTraceId);

        //Then
        assertThat(aggregatedTrace.getKey()).isEqualTo("46876d22cf0f94e0");
        assertThat(aggregatedTrace.spans()).hasSize(2);

        //When
        aggregatedTrace.include(sampleWeather);

        //Then
        assertThat(aggregatedTrace.getKey()).isEqualTo("46876d22cf0f94e0");
        assertThat(aggregatedTrace.spans()).hasSize(3);

        assertThat(aggregatedTrace.root()).isPresent().get()
                .extracting("traceId", "id", "kind")
                .containsExactly("46876d22cf0f94e0", "46876d22cf0f94e0", SERVER);

    }


    @Test
    void shouldLinkSpans() {
        //Given
        NodeTrace sampleZipkin = readTraceFromFile("span-zipkin-sample.json");
        NodeTrace sampleWeather = readTraceFromFile("span-zipkin-weather-sample.json");

        AggregatedTrace aggregatedTrace = AggregatedTrace.withInitial(sampleZipkin, NodeTrace::getTraceId);
        aggregatedTrace.include(sampleWeather);

    }

    NodeTrace readTraceFromFile(final String fileName) {
        InputStream inputStream = Optional.ofNullable(classLoader.getResourceAsStream(fileName))
                .orElseThrow(() -> new RuntimeException("It could not read resource to load span"));
        try {
            return opentracingDeserializer.deserialize(null, IOUtils.toByteArray(inputStream));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}