package com.github.fmcejudo.kafka.extensions.opentracing;

import com.github.fmcejudo.kafka.extensions.opentracing.serialization.SpanSerde;
import com.github.fmcejudo.kafka.extensions.opentracing.serialization.TraceSerde;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class OpentracingSerdesTest {

    @Test
    void shouldRetrieveAOpentracingSerde() {
        //Given && When && Then
        assertThat(OpentracingSerdes.traceSerde()).isNotNull().isInstanceOf(TraceSerde.class);
    }

    @Test
    void shouldRetrieveASpanSerde() {
        //Given && When && Then
        assertThat(OpentracingSerdes.spanSerde()).isNotNull().isInstanceOf(SpanSerde.class);
    }

}