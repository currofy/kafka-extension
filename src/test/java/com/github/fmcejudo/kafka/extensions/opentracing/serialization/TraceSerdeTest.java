package com.github.fmcejudo.kafka.extensions.opentracing.serialization;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import zipkin2.Span;

import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.anyMap;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class TraceSerdeTest {

    @Mock
    TraceSerializer traceSerializer;

    @Mock
    TraceDeserializer traceDeserializer;

    TraceSerde traceSerde;

    @BeforeEach
    void setUp() {
        traceSerde = new TraceSerde(traceSerializer, traceDeserializer);
    }

    @Test
    void shouldSerializeWithOpentracingSerializer() {
        //Given
        TraceSerde specificTraceSerde = new TraceSerde();

        // When
        Serializer<List<Span>> serializer = specificTraceSerde.serializer();

        //Then
        assertThat(serializer).isInstanceOf(TraceSerializer.class);
    }

    @Test
    void shouldDeserializeWithOpentracingDeserializer() {
        //Given
        TraceSerde specificTraceSerde = new TraceSerde();

        // When
        Deserializer<List<Span>> deserializer = specificTraceSerde.deserializer();

        //Then
        assertThat(deserializer).isInstanceOf(TraceDeserializer.class);
    }

    @Test
    public void shouldConfigureSerde() {
        //Given
        doNothing().when(traceSerializer).configure(anyMap(), anyBoolean());
        doNothing().when(traceDeserializer).configure(anyMap(), anyBoolean());

        //When
        traceSerde.configure(Collections.emptyMap(), true);

        //Then
        verify(traceSerializer, times(1)).configure(anyMap(), anyBoolean());
        verify(traceDeserializer, times(1)).configure(anyMap(), anyBoolean());
    }

    @Test
    public void shouldCloseSerde() {
        //Given
        doNothing().when(traceSerializer).close();
        doNothing().when(traceDeserializer).close();

        //When
        traceSerde.close();

        //Then
        verify(traceSerializer, times(1)).close();
        verify(traceDeserializer, times(1)).close();
    }

}