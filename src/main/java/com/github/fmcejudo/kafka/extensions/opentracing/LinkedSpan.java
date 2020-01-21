package com.github.fmcejudo.kafka.extensions.opentracing;

import zipkin2.Span;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public class LinkedSpan {

    private final Span span;

    private LinkedSpan nextServer;

    private LinkedSpan client;

    private LinkedSpan(final Span span) {
        this.span = span;
    }

    public static LinkedSpan of(final Span span) {
        return new LinkedSpan(span);
    }

    public boolean isRoot() {
        return span.parentId() == null;
    }

    public void linkTo(final LinkedSpan linkedSpan) {
        if (linkedSpan == null) {
            return;
        }
        if (this.span == linkedSpan.span) {
            throw new RuntimeException("Linking next span to itself is causing a loop");
        }
        if (linkedSpan.kind() == Span.Kind.CLIENT) {
            if (client != null) {
                throw new RuntimeException("Next span has been already linked to this span");
            }
            this.client = linkedSpan;
        } else if (linkedSpan.kind() == Span.Kind.SERVER) {
            if (nextServer != null) {
                throw new RuntimeException("Next span has been already linked to this span");
            }
            this.nextServer = linkedSpan;
        }
    }

    public String traceId() {
        return this.span.traceId();
    }

    public String id() {
        return span.id();
    }

    public String parentId() {
        return span.parentId();
    }

    public Span.Kind kind() {
        return span.kind();
    }

    public String localServiceName() {
        return this.span.localServiceName();
    }

    public Map<String, String> tags() {
        return this.span.tags();
    }

    public LinkedSpan nextServer() {
        return this.nextServer;
    }

    public LinkedSpan client() {
        return this.client;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LinkedSpan that = (LinkedSpan) o;
        return span.traceId().equals(that.span.traceId())
                && span.id().equals(that.span.id())
                && span.kind() == that.span.kind()
                && span.localServiceName().equals(that.span.localServiceName());
    }

    @Override
    public int hashCode() {
        return Objects.hash(span);
    }

    @Override
    public String toString() {
        return "LinkedSpan{" +
                "span=" + spanToString(span) +
                ", nextServer=" + Optional.ofNullable(nextServer).map(l -> spanToString(l.span)).orElse("-") +
                ", client=" + Optional.ofNullable(client).map(l -> spanToString(l.span)).orElse("-") +
                '}';
    }

    private String spanToString(final Span span) {
        if (span == null) {
            return "-";
        }
        return "traceId: " + span.traceId() +
                ", parentId:" + span.parentId() +
                ", id:" + span.id() +
                ", kind:" + span.kind().name() +
                ", serviceName:" + span.localServiceName();
    }
}
