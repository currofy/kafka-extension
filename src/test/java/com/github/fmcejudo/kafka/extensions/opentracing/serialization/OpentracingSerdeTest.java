package com.github.fmcejudo.kafka.extensions.opentracing.serialization;

import com.github.fmcejudo.kafka.extensions.opentracing.NodeTrace;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.anyMap;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class OpentracingSerdeTest {

    @Mock
    OpentracingSerializer opentracingSerializer;

    @Mock
    OpentracingDeserializer opentracingDeserializer;

    OpentracingSerde opentracingSerde;

    @BeforeEach
    void setUp() {
        opentracingSerde = new OpentracingSerde(opentracingSerializer, opentracingDeserializer);
    }

    @Test
    void shouldSerializeWithOpentracingSerializer() {
        //Given
        OpentracingSerde specificOpentracingSerde = new OpentracingSerde();

        // When
        Serializer<NodeTrace> serializer = specificOpentracingSerde.serializer();

        //Then
        assertThat(serializer).isInstanceOf(OpentracingSerializer.class);
    }

    @Test
    void shouldDeserializeWithOpentracingDeserializer() {
        //Given
        OpentracingSerde specificOpentracingSerde = new OpentracingSerde();

        // When
        Deserializer<NodeTrace> deserializer = specificOpentracingSerde.deserializer();

        //Then
        assertThat(deserializer).isInstanceOf(OpentracingDeserializer.class);
    }

    @Test
    public void shouldConfigureSerde() {
        //Given
        doNothing().when(opentracingSerializer).configure(anyMap(), anyBoolean());
        doNothing().when(opentracingDeserializer).configure(anyMap(), anyBoolean());

        //When
        opentracingSerde.configure(Collections.emptyMap(), true);

        //Then
        verify(opentracingSerializer, times(1)).configure(anyMap(), anyBoolean());
        verify(opentracingDeserializer, times(1)).configure(anyMap(), anyBoolean());
    }

    @Test
    public void shouldCloseSerde() {
        //Given
        doNothing().when(opentracingSerializer).close();
        doNothing().when(opentracingDeserializer).close();

        //When
        opentracingSerde.close();

        //Then
        verify(opentracingSerializer, times(1)).close();
        verify(opentracingDeserializer, times(1)).close();
    }

}