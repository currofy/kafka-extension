package com.github.fmcejudo.kafka.extensions.group;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.springframework.util.Assert;
import zipkin2.Span;

import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static zipkin2.Span.Kind.SERVER;

@JsonIgnoreProperties(ignoreUnknown = true)
public class LinkedSpanList {

    private String key;
    private LinkedSpan root;
    private LinkedList<LinkedSpan> linkedSpans = new LinkedList<>();

    public LinkedSpanList include(final List<LinkedSpan> linkedSpans) {
        LinkedSpanList linkedSpanList = this;
        for (LinkedSpan linkedSpan : linkedSpans) {
            linkedSpanList = linkedSpanList.include(linkedSpan);
        }
        return linkedSpanList;
    }

    public synchronized LinkedSpanList include(final LinkedSpan linkedSpan) {
        if (key == null) {
            this.key = linkedSpan.getTraceId();
        }
        Assert.isTrue(this.key.equals(linkedSpan.getTraceId()), "To aggregate a trace the keys should match");
        this.add(linkedSpan);
        return this;
    }

    private void add(final LinkedSpan linkedSpan) {
        if (linkedSpan.isRoot()) {
            this.root = linkedSpan;
        }
        this.linkedSpans.add(linkedSpan);
    }

    public LinkedList<LinkedSpan> getLinkedSpans() {
        return this.linkedSpans;
    }

    public String getKey() {
        return this.key;
    }

    public Stream<LinkedSpan> allTraces() {
        return new TraceStream(this).stream();
    }

    public Stream<LinkedSpan> httpTraces() {
        return new TraceStream(this).stream()
                .filter(s -> asList(SERVER, Span.Kind.CLIENT).contains(s.getKind()));
    }

    public Stream<LinkedSpan> unclassifiedTraces() {
        return new TraceStream(this).stream().filter(s -> s.getKind() == null);
    }

    private static final class TraceStream {

        private LinkedSpan[] linkedSpans;


        TraceStream(final LinkedSpanList linkedSpanList) {
            this.linkedSpans = linkedSpanList.linkedSpans.toArray(new LinkedSpan[0]);
        }

        Stream<LinkedSpan> stream() {
            for (LinkedSpan linkedSpan : linkedSpans) {
                findSpan(linkedSpan.getParentId(), Stream.of(linkedSpans).filter(s -> s.getKind().equals(SERVER)))
                        .ifPresent(ls -> ls.linkTo(linkedSpan));
            }
            return Stream.of(linkedSpans);
        }


        private Optional<LinkedSpan> findSpan(final String parentId, Stream<LinkedSpan> linkedSpanList) {
            return linkedSpanList.filter(ls -> ls.getId().equals(parentId)).findFirst();
        }
    }


}
