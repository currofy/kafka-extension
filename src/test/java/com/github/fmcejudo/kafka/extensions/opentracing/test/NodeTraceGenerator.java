package com.github.fmcejudo.kafka.extensions.opentracing.test;

import net.andreinc.mockneat.MockNeat;
import net.andreinc.mockneat.unit.regex.Regex;
import zipkin2.Endpoint;
import zipkin2.Span;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static zipkin2.Span.Kind.CLIENT;
import static zipkin2.Span.Kind.SERVER;

public class NodeTraceGenerator {


    private MockNeat mockNeat;

    private NodeTraceGenerator() {
        mockNeat = MockNeat.threadLocal();
    }

    public static NodeTraceGenerator generator() {
        return new NodeTraceGenerator();
    }

    public List<Span> simulateSequentialHTTPCalls(final String... services) {
        if (services.length == 0) {
            throw new RuntimeException("No services to request");
        }

        Map<String, ArrayList<Span>> logsInNodeMap =
                Stream.of(services).collect(Collectors.toMap(String::toLowerCase, k -> new ArrayList<Span>()));

        Span rootSpan = generateRootSpan(services[0]);
        addSpanToNode(logsInNodeMap, rootSpan);

        Span aux = rootSpan;
        for (int i = 1; i < services.length; i++) {
            Span children = generateServerSpan(rootSpan.traceId(), aux.id(), services[i]);
            addSpanToNode(logsInNodeMap, children);

            Span clientResponse = generateClientSpan(children, aux.localServiceName());
            addSpanToNode(logsInNodeMap, clientResponse);
            aux = children;
        }

        return logsInNodeMap.values().stream().flatMap(Collection::stream).collect(toList());
    }


    public Span generateRootSpan(final String serviceName) {
        Regex idPattern = mockNeat.regex("[0-9a-f]{16}");
        return generateServerSpan(idPattern.val(), null, serviceName);
    }

    private Span generateServerSpan(final String traceId, final String parentId, final String serviceName) {
        Regex idPattern = mockNeat.regex("[0-9a-f]{16}");
        return generateSpanCommon(traceId, idPattern.val(), parentId, serviceName).kind(SERVER).build();
    }

    private Span generateClientSpan(final Span span, final String serviceName) {
        return generateSpanCommon(span.traceId(), span.id(), span.parentId(), serviceName)
                .kind(CLIENT).build();
    }

    private Span.Builder generateSpanCommon(final String traceId, final String id,
                                            final String parentId, final String serviceName) {
        return Span.newBuilder()
                .parentId(parentId)
                .id(id)
                .traceId(traceId)
                .putTag("author", "fmcejudo")
                .putTag("version", "1.0")
                .localEndpoint(Endpoint.newBuilder().serviceName(serviceName).build());
    }


    private void addSpanToNode(Map<String, ArrayList<Span>> logsInNodeMap, Span rootSpan) {
        logsInNodeMap.computeIfPresent(rootSpan.localServiceName(), (key, list) -> {
            list.add(rootSpan);
            return list;
        });
    }

}
