package com.github.fmcejudo.kafka.extensions.opentracing.converter;

import com.github.fmcejudo.kafka.extensions.opentracing.Trace;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.MessageBuilder;
import zipkin2.Span;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static java.util.Collections.singletonMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
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
        assertThat(opentracingConverter.supports(Trace.class)).isTrue();
        assertThat(opentracingConverter.supports(CustomTrace.class)).isFalse();
    }

    @Test
    void shouldConvertIntoTrace() throws IOException {
        //Given
        InputStream inputStream = Optional.ofNullable(classLoader.getResourceAsStream("span.json"))
                .orElseThrow(() -> new RuntimeException("It could not read resource to load span"));

        byte[] spanBytes = IOUtils.toByteArray(inputStream);
        Message<?> message = MessageBuilder.createMessage(spanBytes, new MessageHeaders(Collections.emptyMap()));

        //When
        Object trace = opentracingConverter.convertFromInternal(message, Trace.class, null);

        //Then
        assertThat(trace).isInstanceOf(Trace.class);
        assertThat((Trace) trace)
                .isNotNull()
                .extracting("traceId").isEqualTo("d566efbd4dc4a74f");
        assertThat(((Trace) trace).getSpans()).hasSize(2);
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
                .duration(4583L)
                .name("my_service").build();
        //When
        Object o = opentracingConverter.convertToInternal(
                Trace.builder().traceId(traceId).spans(Collections.singletonList(span)).build(),
                new MessageHeaders(singletonMap(CONTENT_TYPE, "application/opentracing")),
                null
        );

        //Then
        assertThat(o).isInstanceOf(Trace.class);
    }


    static class CustomTrace extends Trace {

        public CustomTrace(String traceId, List<Span> spans) {
            super(traceId, spans);
        }
    }
}