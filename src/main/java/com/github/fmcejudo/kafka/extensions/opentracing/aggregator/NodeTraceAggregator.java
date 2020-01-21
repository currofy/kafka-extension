package com.github.fmcejudo.kafka.extensions.opentracing.aggregator;

import com.github.fmcejudo.kafka.extensions.opentracing.AggregatedTrace;
import com.github.fmcejudo.kafka.extensions.opentracing.NodeTrace;
import org.apache.kafka.streams.kstream.Aggregator;

public class NodeTraceAggregator implements Aggregator<String, NodeTrace, AggregatedTrace> {

    @Override
    public AggregatedTrace apply(String s, NodeTrace nodeTrace, AggregatedTrace aggregatedTrace) {
        if (aggregatedTrace == null) {
            return AggregatedTrace.withInitial(nodeTrace, NodeTrace::getTraceId);
        } else {
            return aggregatedTrace.include(nodeTrace);
        }
    }
}
