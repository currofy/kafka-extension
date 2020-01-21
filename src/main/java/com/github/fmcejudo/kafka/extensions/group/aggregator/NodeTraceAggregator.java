package com.github.fmcejudo.kafka.extensions.group.aggregator;

import com.github.fmcejudo.kafka.extensions.group.LinkedSpan;
import com.github.fmcejudo.kafka.extensions.group.LinkedSpanList;
import com.github.fmcejudo.kafka.extensions.opentracing.NodeTrace;
import org.apache.kafka.streams.kstream.Aggregator;

import java.util.List;
import java.util.stream.Collectors;

public class NodeTraceAggregator implements Aggregator<String, NodeTrace, LinkedSpanList> {

    @Override
    public LinkedSpanList apply(String s, NodeTrace nodeTrace, LinkedSpanList linkedSpanList) {
        List<LinkedSpan> spans = nodeTrace.getSpans().stream().map(LinkedSpan::of).collect(Collectors.toList());
        return linkedSpanList.include(spans);
    }
}
