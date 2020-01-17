package com.github.fmcejudo.kafka.extensions.opentracing;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import zipkin2.Span;

import java.util.List;
import java.util.stream.Collectors;

@Data
@AllArgsConstructor
@Builder
public class Trace {

    private final String traceId;

    private final List<Span> spans;

    public boolean containsRoot() {
        return spans.stream().anyMatch(s -> s.parentId() == null || s.parentId().trim().length() == 0);
    }

    public List<Span> serverSpans() {
        return spans.stream()
                .filter(s -> s.kind().equals(Span.Kind.SERVER))
                .collect(Collectors.toList());
    }

    public long duration() {
        return serverSpans().stream()
                .map(Span::duration).reduce(Long::sum)
                .orElseThrow(() -> new RuntimeException("No SERVER spans found, but required"));
    }

    public List<String> serviceNames() {
        return serverSpans().stream()
                .map(Span::localServiceName)
                .collect(Collectors.toList());
    }

}
