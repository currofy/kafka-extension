package com.github.fmcejudo.kafka.extensions.opentracing;

import com.github.fmcejudo.kafka.extensions.opentracing.test.NodeTraceGenerator;
import org.junit.jupiter.api.Test;
import zipkin2.Endpoint;
import zipkin2.Span;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class TraceStreamTest {

    private final String traceId = "0000000000000001";

    @Test
    void shouldFindARoot() {
        //Given
        AggregatedTrace aggregatedTrace = AggregatedTrace.withInitial(buildRootLinkedSpan(), NodeTrace::getTraceId)
                .include(buildRootChildrenLinkedSpan())
                .include(buildSecondLevelChildrenLinkedSpan());

        //When
        Optional<LinkedSpan> rootLinkedSpanOpt =
                aggregatedTrace.allTraces().filter(ls -> ls.parentId() == null).findFirst();

        //Then
        assertThat(rootLinkedSpanOpt).isPresent().get()
                .extracting("id").isEqualTo(traceId);

        LinkedSpan rootLinkedSpan = rootLinkedSpanOpt.get();

        assertThat(rootLinkedSpan.nextServer()).isNotNull().extracting("id").isEqualTo("0000000000000002");
        assertThat(rootLinkedSpan.nextServer().nextServer()).isNotNull().extracting("id").isEqualTo("0000000000000003");
        assertThat(rootLinkedSpan.nextServer().nextServer().nextServer()).isNull();
    }

    @Test
    void shouldNotFindTraces() {
        //Given
        AggregatedTrace aggregatedTrace = AggregatedTrace.withInitial(buildRootLinkedSpan(), NodeTrace::getTraceId)
                .include(buildRootChildrenLinkedSpan())
                .include(buildSecondLevelChildrenLinkedSpan());

        //When
        Stream<LinkedSpan> linkedSpanStream = aggregatedTrace.unclassifiedTraces();

        //Then
        assertThat(linkedSpanStream).isEmpty();
    }

    @Test
    void shouldFindTracesWithClients() {
        //Given
        NodeTraceGenerator generator = NodeTraceGenerator.generator();

        List<NodeTrace> nodeTraces = generator.simulateSequentialHTTPCalls("servicea", "serviceb", "servicec");

        //When
        AggregatedTrace aggregatedTrace =
                AggregatedTrace.withInitial(nodeTraces.remove(0), NodeTrace::getTraceId).include(nodeTraces);


        //Then
        assertThat(aggregatedTrace.httpTraces().filter(l -> l.localServiceName().equals("servicea")))
                .hasSize(2)
                .extractingResultOf("kind").containsExactly(Span.Kind.SERVER, Span.Kind.CLIENT);

        assertThat(aggregatedTrace.httpTraces().filter(l -> l.localServiceName().equals("servicec")))
                .hasSize(1)
                .extractingResultOf("kind").containsExactly(Span.Kind.SERVER);

        LinkedSpan rootLinkedSpan = aggregatedTrace.httpTraces()
                .filter(l -> l.kind().equals(Span.Kind.SERVER) && l.localServiceName().equals("servicea"))
                .findFirst().orElseThrow(() -> new RuntimeException("Root span should be present"));

        LinkedSpan rootChildLinkedSpan = rootLinkedSpan.nextServer();
        assertThat(rootChildLinkedSpan).isNotNull()
                .extracting("localServiceName", "parentId", "traceId", "kind")
                .containsExactly("serviceb", rootLinkedSpan.id(), rootLinkedSpan.traceId(), Span.Kind.SERVER);

        assertThat(rootLinkedSpan.tags()).containsKeys("author", "version");

        assertThat(rootLinkedSpan.client()).isNotNull()
                .extracting("localServiceName", "parentId", "traceId", "kind", "id")
                .containsExactly(
                        "servicea", rootLinkedSpan.id(), rootLinkedSpan.traceId(),
                        Span.Kind.CLIENT, rootChildLinkedSpan.id()
                );

    }

    private NodeTrace buildRootLinkedSpan() {
        Span rootSpan = Span.newBuilder()
                .traceId(traceId)
                .id(traceId)
                .localEndpoint(Endpoint.newBuilder().serviceName("serviceA").build())
                .kind(Span.Kind.SERVER)
                .build();
        return NodeTrace.from(Collections.singletonList(rootSpan));
    }

    private NodeTrace buildRootChildrenLinkedSpan() {
        Span firstLevelSpan = Span.newBuilder()
                .traceId(traceId)
                .parentId(traceId)
                .id("0000000000000002")
                .localEndpoint(Endpoint.newBuilder().serviceName("serviceB").build())
                .kind(Span.Kind.SERVER)
                .build();
        return NodeTrace.from(Collections.singletonList(firstLevelSpan));
    }

    private NodeTrace buildSecondLevelChildrenLinkedSpan() {
        Span secondLevelSpan = Span.newBuilder()
                .traceId(traceId)
                .parentId("0000000000000002")
                .id("0000000000000003")
                .localEndpoint(Endpoint.newBuilder().serviceName("serviceC").build())
                .kind(Span.Kind.SERVER)
                .build();
        return NodeTrace.from(Collections.singletonList(secondLevelSpan));
    }

}