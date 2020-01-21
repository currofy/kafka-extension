package com.github.fmcejudo.kafka.extensions.opentracing.converter;

import com.github.fmcejudo.kafka.extensions.opentracing.NodeTrace;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.MessageBuilder;
import zipkin2.Endpoint;
import zipkin2.Span;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Optional;

import static java.util.Collections.singletonMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.messaging.MessageHeaders.CONTENT_TYPE;

class OpentracingConverterTest {

    OpentracingConverter opentracingConverter;

    private ClassLoader classLoader;

    @BeforeEach
    void setUp() {
        opentracingConverter = new OpentracingConverter();
        classLoader = this.getClass().getClassLoader();
    }

    @Test
    void shouldSupportOnlyTraces() {

        assertThat(opentracingConverter.supports(String.class)).isFalse();
        assertThat(opentracingConverter.supports(NodeTrace.class)).isTrue();
    }

    @Test
    void shouldConvertIntoTrace() throws IOException {
        //Given
        InputStream inputStream = Optional.ofNullable(classLoader.getResourceAsStream("span-zipkin-sample.json"))
                .orElseThrow(() -> new RuntimeException("It could not read resource to load span"));

        byte[] spanBytes = IOUtils.toByteArray(inputStream);
        Message<?> message = MessageBuilder.createMessage(spanBytes, new MessageHeaders(Collections.emptyMap()));

        //When
        Object trace = opentracingConverter.convertFromInternal(message, NodeTrace.class, null);

        //Then
        assertThat(trace).isInstanceOf(NodeTrace.class);
        assertThat((NodeTrace) trace)
                .isNotNull()
                .extracting("traceId").isEqualTo("46876d22cf0f94e0");
        assertThat(((NodeTrace) trace).getSpans()).hasSize(2);
    }

    @Test
    void shouldStringifyTrace() {
        //Given
        String traceId = "0a0a0a0a";
        String spanId = "faf1faf0";

        Span span = Span.newBuilder()
                .traceId(traceId)
                .id(spanId)
                .kind(Span.Kind.SERVER)
                .localEndpoint(Endpoint.newBuilder().serviceName("serviceA").build())
                .duration(4583L)
                .name("my_service").build();
        //When
        Object o = opentracingConverter.convertToInternal(
                NodeTrace.from(Collections.singletonList(span)),
                new MessageHeaders(singletonMap(CONTENT_TYPE, "application/opentracing")),
                null
        );

        //Then
        assertThat(o).isInstanceOf(NodeTrace.class);
    }

}