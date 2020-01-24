package com.github.fmcejudo.kafka.extensions.group.aggregator;

import com.github.fmcejudo.kafka.extensions.group.LinkedSpan;
import com.github.fmcejudo.kafka.extensions.group.LinkedSpanList;
import org.apache.kafka.streams.kstream.Aggregator;
import zipkin2.Span;

public class GroupSpanAggregator implements Aggregator<String, Span, LinkedSpanList> {

    @Override
    public LinkedSpanList apply(String s, Span span, LinkedSpanList linkedSpanList) {
        return linkedSpanList.include(LinkedSpan.of(span));
    }
}
