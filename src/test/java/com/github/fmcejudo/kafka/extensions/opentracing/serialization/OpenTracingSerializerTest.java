package com.github.fmcejudo.kafka.extensions.opentracing.serialization;

import com.github.fmcejudo.kafka.extensions.opentracing.test.NodeTraceGenerator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import zipkin2.Endpoint;
import zipkin2.Span;
import zipkin2.codec.SpanBytesEncoder;

import java.nio.charset.StandardCharsets;
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
                .localEndpoint(Endpoint.newBuilder().serviceName("serviceA").build())
                .duration(4583L)
                .name("my_service").build();


        //When
        byte[] json2ZipkinSpan = opentracingSerializer.serialize(topic, Collections.singletonList(span));

        //Then
        assertThat(new String(json2ZipkinSpan))
                .contains("traceId").contains(traceId)
                .contains("\"id\"").contains(spanId)
                .contains("kind").contains(Span.Kind.SERVER.name())
                .contains("name").contains("my_service");
    }


    //TODO(cejudogomezf) it should do a ParamMock test, which should assert each type of span encoder
    @Test
    void shouldSelectAnEncoder() {
        //Given
        Span rootSpan = NodeTraceGenerator.generator().generateRootSpan("serviceA");
        assertThat(opentracingSerializer.bytesEncoder).isEqualTo(SpanBytesEncoder.JSON_V2);

        //When
        opentracingSerializer.configure(Collections.singletonMap("span.encoder", "json_v1"), false);
        //Then
        assertThat(opentracingSerializer.bytesEncoder).isEqualTo(SpanBytesEncoder.JSON_V1);
        byte[] serialize = opentracingSerializer.serialize(null, Collections.singletonList(rootSpan));
        System.out.println(new String(serialize, StandardCharsets.UTF_8));

        //When
        opentracingSerializer.configure(Collections.singletonMap("span.encoder", "thrift"), false);
        //Then
        assertThat(opentracingSerializer.bytesEncoder).isEqualTo(SpanBytesEncoder.THRIFT);

        //When
        opentracingSerializer.configure(Collections.singletonMap("span.encoder", "proto3"), false);
        //Then
        assertThat(opentracingSerializer.bytesEncoder).isEqualTo(SpanBytesEncoder.PROTO3);

        //When
        opentracingSerializer.configure(Collections.singletonMap("span.encoder", "invalid"), false);
        //Then
        assertThat(opentracingSerializer.bytesEncoder).isEqualTo(SpanBytesEncoder.JSON_V2);
    }

}