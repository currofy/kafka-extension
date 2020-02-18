package com.currofy.kafka.extensions.opentracing.serialization;

import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import zipkin2.Endpoint;
import zipkin2.Span;

import static org.assertj.core.api.Assertions.assertThat;

class SpanSerializerTest {


    private final String topic = "mytopic";

    SpanSerializer spanSerializer;

    @BeforeEach
    void setUp() {
        spanSerializer = new SpanSerializer();
    }

    @Test
    void shouldSerializeASpanIntoString() {

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
        byte[] json2ZipkinSpan = spanSerializer.serialize(topic, span);

        //Then
        assertThat(new String(json2ZipkinSpan))
                .contains("traceId").contains(traceId)
                .contains("\"id\"").contains(spanId)
                .contains("kind").contains(Span.Kind.SERVER.name())
                .contains("name").contains("my_service");
    }

    @Test
    void shouldSerializeASpanWithHeaderIntoString() {

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
        RecordHeader[] headerArray = new RecordHeader[]{new RecordHeader("key", "value".getBytes())};
        RecordHeaders headers = new RecordHeaders(headerArray);
        byte[] json2ZipkinSpan = spanSerializer.serialize(topic, headers, span);

        //Then
        assertThat(new String(json2ZipkinSpan))
                .contains("traceId").contains(traceId)
                .contains("\"id\"").contains(spanId)
                .contains("kind").contains(Span.Kind.SERVER.name())
                .contains("name").contains("my_service");
    }


}