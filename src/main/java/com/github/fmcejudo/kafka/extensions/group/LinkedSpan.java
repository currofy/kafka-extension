package com.github.fmcejudo.kafka.extensions.group;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import zipkin2.Span;

import java.util.Map;
import java.util.Optional;

@NoArgsConstructor
@Data
@JsonIgnoreProperties(ignoreUnknown = true)
@Slf4j
public class LinkedSpan {

    private String id;
    private String traceId;
    private String parentId;
    private Span.Kind kind;
    private String localServiceName;
    private Map<String, String> tags;

    private LinkedSpan nextServer;

    private LinkedSpan client;

    private LinkedSpan(final Span span) {
        this.id = span.id();
        this.traceId = span.traceId();
        this.parentId = span.parentId();
        this.kind = span.kind();
        this.localServiceName = span.localServiceName();
        this.tags = span.tags();
    }

    public static LinkedSpan of(final Span span) {
        return new LinkedSpan(span);
    }

    public boolean isRoot() {
        return parentId == null;
    }

    public void linkTo(final LinkedSpan linkedSpan) {
        if (linkedSpan == null) {
            return;
        }
        if (this == linkedSpan) {
            throw new RuntimeException("Linking next span to itself is causing a loop");
        }
        if (linkedSpan.getKind() == Span.Kind.CLIENT) {
            if (client != null && !client.getId().equals(linkedSpan.getId())) {
                throw new RuntimeException("client span has been already linked to this span");
            }
            this.client = linkedSpan;
        } else if (linkedSpan.getKind() == Span.Kind.SERVER) {
            if (nextServer != null && !nextServer.getId().equals(linkedSpan.getId())) {
                throw new RuntimeException("nextServer span has been already linked to this span");
            }
            this.nextServer = linkedSpan;
        }
    }

    public LinkedSpan nextServer() {
        return this.nextServer;
    }

    public LinkedSpan client() {
        return this.client;
    }

    @Override
    public String toString() {
        return "LinkedSpan{" +
                "span=" + spanToString(this) +
                ", nextServer=" + Optional.ofNullable(nextServer).map(this::spanToString).orElse("-") +
                ", client=" + Optional.ofNullable(client).map(this::spanToString).orElse("-") +
                '}';
    }

    private String spanToString(final LinkedSpan linkedSpan) {
        if (linkedSpan == null) {
            return "-";
        }
        return "traceId: " + linkedSpan.getTraceId() +
                ", parentId:" + linkedSpan.getParentId() +
                ", id:" + linkedSpan.getId() +
                ", kind:" + linkedSpan.getKind().name() +
                ", serviceName:" + linkedSpan.getLocalServiceName();
    }

}
