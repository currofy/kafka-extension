package com.github.fmcejudo.kafka.extensions.opentracing;


import org.springframework.util.Assert;
import zipkin2.Span;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class AggregatedTrace {

    private Function<Trace, String> extractKeyFn;

    private final String key;

    private List<Span> spans;

    private AggregatedTrace(final Trace trace, final Function<Trace, String> extractKeyFn) {
        Assert.notNull(trace, "Trace null can not be aggregated");
        Assert.notNull(extractKeyFn, "Function to extract a key is required");
        this.spans = trace.getSpans();
        this.extractKeyFn = extractKeyFn;
        this.key = extractKeyFn.apply(trace);
    }

    public static AggregatedTrace withInitial(final Trace trace, final Function<Trace, String> keyExtractorFn) {
        return new AggregatedTrace(trace, keyExtractorFn);
    }

    public synchronized void add(final Trace trace) {
        Assert.isTrue(this.key.equals(extractKeyFn.apply(trace)), "To aggregate a trace the keys should match");
        spans = Stream.concat(spans.stream(), trace.getSpans().stream()).collect(Collectors.toList());
    }

    public String getKey() {
        return this.key;
    }

    public List<Span> spans() {
        return this.spans;
    }

}
