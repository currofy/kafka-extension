package com.github.fmcejudo.kafka.extensions.opentracing;


import org.springframework.util.Assert;
import zipkin2.Span;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static zipkin2.Span.Kind.SERVER;

public final class AggregatedTrace {

    private Function<NodeTrace, String> extractKeyFn;

    private final String key;

    private List<Span> spans;

    private AggregatedTrace(final NodeTrace nodeTrace, final Function<NodeTrace, String> extractKeyFn) {
        this(nodeTrace.getSpans(), extractKeyFn.apply(nodeTrace), extractKeyFn);
    }

    private AggregatedTrace(final List<Span> spans, final String key,
                            final Function<NodeTrace, String> extractKeyFn) {

        Assert.notNull(spans, "the list of span can not be null");
        Assert.notEmpty(spans, "The list of span can not be empty");
        Assert.notNull(extractKeyFn, "Function to extract a key is required");
        this.spans = spans;
        this.extractKeyFn = extractKeyFn;
        this.key = key;
    }

    public static AggregatedTrace withInitial(final NodeTrace nodeTrace,
                                              final Function<NodeTrace, String> keyExtractorFn) {

        return new AggregatedTrace(nodeTrace, keyExtractorFn);
    }

    public AggregatedTrace include(final List<NodeTrace> nodeTraces) {
        AggregatedTrace aggregatedTrace = this;
        for (NodeTrace nodeTrace : nodeTraces) {
            aggregatedTrace = aggregatedTrace.include(nodeTrace);
        }
        return aggregatedTrace;
    }

    public synchronized AggregatedTrace include(final NodeTrace nodeTrace) {
        Assert.isTrue(this.key.equals(extractKeyFn.apply(nodeTrace)), "To aggregate a trace the keys should match");
        spans = Stream.concat(spans.stream(), nodeTrace.getSpans().stream()).collect(Collectors.toList());
        return new AggregatedTrace(spans, key, extractKeyFn);
    }

    public String getKey() {
        return this.key;
    }

    public List<Span> spans() {
        return this.spans;
    }

    public Optional<Span> root() {
        List<Span> rootSpans = spans.stream().filter(s -> s.parentId() == null).collect(Collectors.toList());
        if (rootSpans.size() > 1) {
            throw new RuntimeException("Correlated spans should have as much only one parent");
        }
        return rootSpans.stream().findFirst();
    }

    public Stream<LinkedSpan> allTraces() {
        return new TraceStream(this).stream();
    }

    public Stream<LinkedSpan> httpTraces() {
        return new TraceStream(this).stream()
                .filter(s -> asList(SERVER, Span.Kind.CLIENT).contains(s.kind()));
    }

    public Stream<LinkedSpan> unclassifiedTraces() {
        return new TraceStream(this).stream().filter(s -> s.kind() == null);
    }

    private static final class TraceStream {

        private final AggregatedTrace aggregatedTrace;

        TraceStream(final AggregatedTrace aggregatedTrace) {
            this.aggregatedTrace = aggregatedTrace;
        }

        Stream<LinkedSpan> stream() {
            LinkedSpan[] linkedSpans = aggregatedTrace.spans().stream()
                    .map(LinkedSpan::of).toArray(LinkedSpan[]::new);

            for (LinkedSpan linkedSpan : linkedSpans) {
                findSpan(linkedSpan.parentId(), Stream.of(linkedSpans).filter(s -> s.kind().equals(SERVER)))
                        .ifPresent(ls -> ls.linkTo(linkedSpan));
            }
            return Stream.of(linkedSpans);
        }

        private Optional<LinkedSpan> findSpan(final String parentId, Stream<LinkedSpan> linkedSpanList) {
            return linkedSpanList.filter(ls -> ls.id().equals(parentId)).findFirst();
        }

    }

}
