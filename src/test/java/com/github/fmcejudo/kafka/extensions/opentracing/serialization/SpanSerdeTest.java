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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class SpanSerdeTest {


    @Mock
    SpanSerializer spanSerializer;

    @Mock
    SpanDeserializer spanDeserializer;

    SpanSerde spanSerde;

    @BeforeEach
    void setUp() {
        spanSerde = new SpanSerde(spanSerializer, spanDeserializer);
    }

    @Test
    void shouldSerializeWithSpanSerializer() {
        //Given
        SpanSerde customSpanSerde = new SpanSerde();

        // When
        Serializer<Span> spanSerializer = customSpanSerde.serializer();

        //Then
        assertThat(spanSerializer).isInstanceOf(SpanSerializer.class);
    }

    @Test
    void shouldDeserializeWithSpanDeserializer() {
        //Given
        SpanSerde customSpanSerde = new SpanSerde();

        // When
        Deserializer<Span> spanSerializer = customSpanSerde.deserializer();

        //Then
        assertThat(spanSerializer).isInstanceOf(SpanDeserializer.class);;
    }

    @Test
    public void shouldConfigureSerde() {
        //Given
        doNothing().when(spanSerializer).configure(anyMap(), anyBoolean());
        doNothing().when(spanDeserializer).configure(anyMap(), anyBoolean());

        //When
        spanSerde.configure(Collections.emptyMap(), true);

        //Then
        verify(spanSerializer, times(1)).configure(anyMap(), anyBoolean());
        verify(spanDeserializer, times(1)).configure(anyMap(), anyBoolean());
    }

    @Test
    public void shouldCloseSerde() {
        //Given
        doNothing().when(spanSerializer).close();
        doNothing().when(spanDeserializer).close();

        //When
        spanSerde.close();

        //Then
        verify(spanSerializer, times(1)).close();
        verify(spanDeserializer, times(1)).close();
    }

}