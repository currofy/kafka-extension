package com.github.fmcejudo.kafka.extensions.opentracing;

import com.github.fmcejudo.kafka.extensions.group.LinkedSpan;
import com.github.fmcejudo.kafka.extensions.group.LinkedSpanList;
import com.github.fmcejudo.kafka.extensions.opentracing.test.NodeTraceGenerator;
import org.junit.jupiter.api.Test;
import zipkin2.Endpoint;
import zipkin2.Span;

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;

class TraceStreamTest {

    private final String traceId = "0000000000000001";

    @Test
    void shouldFindARoot() {
        //Given
        LinkedSpanList linkedSpanList = new LinkedSpanList();
        linkedSpanList.include(buildRootLinkedSpan());
        linkedSpanList.include(buildRootChildrenLinkedSpan());
        linkedSpanList.include(buildSecondLevelChildrenLinkedSpan());


        //When
        Optional<LinkedSpan> rootLinkedSpanOpt =
                linkedSpanList.allTraces().filter(ls -> ls.getParentId() == null).findFirst();

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
        LinkedSpanList linkedSpanList = new LinkedSpanList();
        linkedSpanList.include(buildRootLinkedSpan());
        linkedSpanList.include(buildRootChildrenLinkedSpan());
        linkedSpanList.include(buildSecondLevelChildrenLinkedSpan());


        //When
        Stream<LinkedSpan> linkedSpanStream = linkedSpanList.unclassifiedTraces();

        //Then
        assertThat(linkedSpanStream).isEmpty();
    }

    @Test
    void shouldFindTracesWithClients() {
        //Given
        NodeTraceGenerator generator = NodeTraceGenerator.generator();

        List<Span> spans = generator.simulateSequentialHTTPCalls("servicea", "serviceb", "servicec");

        LinkedSpanList linkedSpanList = new LinkedSpanList();

        //When
        LinkedSpanList spanList = linkedSpanList.include(spans.stream().map(LinkedSpan::of).collect(toList()));

        //Then
        assertThat(spanList.httpTraces().filter(l -> l.getLocalServiceName().equals("servicea")))
                .hasSize(2)
                .extractingResultOf("getKind").containsExactly(Span.Kind.SERVER, Span.Kind.CLIENT);

        assertThat(spanList.httpTraces().filter(l -> l.getLocalServiceName().equals("servicec")))
                .hasSize(1)
                .extractingResultOf("getKind").containsExactly(Span.Kind.SERVER);

        LinkedSpan rootLinkedSpan = spanList.httpTraces()
                .filter(l -> l.getKind().equals(Span.Kind.SERVER) && l.getLocalServiceName().equals("servicea"))
                .findFirst().orElseThrow(() -> new RuntimeException("Root span should be present"));

        LinkedSpan rootChildLinkedSpan = rootLinkedSpan.nextServer();
        assertThat(rootChildLinkedSpan).isNotNull()
                .extracting("localServiceName", "parentId", "traceId", "kind")
                .containsExactly("serviceb", rootLinkedSpan.getId(), rootLinkedSpan.getTraceId(), Span.Kind.SERVER);

        assertThat(rootLinkedSpan.getTags()).containsKeys("author", "version");

        assertThat(rootLinkedSpan.client()).isNotNull()
                .extracting("localServiceName", "parentId", "traceId", "kind", "id")
                .containsExactly(
                        "servicea", rootLinkedSpan.getId(), rootLinkedSpan.getTraceId(),
                        Span.Kind.CLIENT, rootChildLinkedSpan.getId()
                );

    }

    private LinkedSpan buildRootLinkedSpan() {
        Span rootSpan = Span.newBuilder()
                .traceId(traceId)
                .id(traceId)
                .localEndpoint(Endpoint.newBuilder().serviceName("serviceA").build())
                .kind(Span.Kind.SERVER)
                .build();

        return LinkedSpan.of(rootSpan);
    }

    private LinkedSpan buildRootChildrenLinkedSpan() {
        Span firstLevelSpan = Span.newBuilder()
                .traceId(traceId)
                .parentId(traceId)
                .id("0000000000000002")
                .localEndpoint(Endpoint.newBuilder().serviceName("serviceB").build())
                .kind(Span.Kind.SERVER)
                .build();
        return LinkedSpan.of(firstLevelSpan);
    }

    private LinkedSpan buildSecondLevelChildrenLinkedSpan() {
        Span secondLevelSpan = Span.newBuilder()
                .traceId(traceId)
                .parentId("0000000000000002")
                .id("0000000000000003")
                .localEndpoint(Endpoint.newBuilder().serviceName("serviceC").build())
                .kind(Span.Kind.SERVER)
                .build();
        return LinkedSpan.of(secondLevelSpan);
    }

}