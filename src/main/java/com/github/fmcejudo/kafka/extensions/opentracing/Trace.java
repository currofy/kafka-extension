package com.github.fmcejudo.kafka.extensions.opentracing;

import lombok.Data;
import zipkin2.Span;

import java.util.Collections;
import java.util.List;

@Data
public class Trace {

    private final String traceId;

    private final List<Span> spans;

    private Trace(final String traceId, final List<Span> spans) {
        this.traceId = traceId;
        this.spans = Collections.unmodifiableList(spans);
    }

    public static Trace from(final List<Span> spans) {
        SpanListValidator.assertSameServiceName(spans);
        return new Trace(SpanTraceIdExtractor.traceId(spans), spans);
    }

    public boolean containsRoot() {
        return spans.stream().anyMatch(s -> s.parentId() == null || s.parentId().trim().length() == 0);
    }

    private static class SpanListValidator {

        private static void assertSameServiceName(final List<Span> spans) {
            String serviceName = spans.get(0).localServiceName();
            if (!spans.stream().map(Span::localServiceName).allMatch(sn -> sn.equals(serviceName))) {
                throw new RuntimeException("All the spans should have the same service name");
            }
        }
    }

    public static class SpanTraceIdExtractor {

        private static String traceId(final List<Span> spans) {
            String traceId = spans.get(0).traceId();
            if (spans.stream().anyMatch(s -> !s.traceId().equals(traceId))) {
                throw new RuntimeException("All spans should have the same trace id: " + traceId);
            }
            return traceId;
        }
    }

}
