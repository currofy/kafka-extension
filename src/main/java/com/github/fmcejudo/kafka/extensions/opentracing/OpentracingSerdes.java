package com.github.fmcejudo.kafka.extensions.opentracing;

import com.github.fmcejudo.kafka.extensions.opentracing.serialization.SpanSerde;
import com.github.fmcejudo.kafka.extensions.opentracing.serialization.TraceSerde;

public class OpentracingSerdes {

    public static TraceSerde traceSerde() {
        return new TraceSerde();
    }

    public static SpanSerde spanSerde() {
        return new SpanSerde();
    }
}
