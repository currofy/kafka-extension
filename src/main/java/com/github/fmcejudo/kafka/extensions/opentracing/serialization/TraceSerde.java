package com.github.fmcejudo.kafka.extensions.opentracing.serialization;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import zipkin2.Span;

import java.util.List;
import java.util.Map;

public class TraceSerde implements Serde<List<Span>> {

    private final TraceSerializer traceSerializer;
    private final TraceDeserializer traceDeserializer;

    public TraceSerde() {
        this(new TraceSerializer(), new TraceDeserializer());
    }

    public TraceSerde(final TraceSerializer traceSerializer,
                      final TraceDeserializer traceDeserializer) {
        this.traceSerializer = traceSerializer;
        this.traceDeserializer = traceDeserializer;
    }

    @Override
    public Serializer<List<Span>> serializer() {
        return traceSerializer;
    }

    @Override
    public Deserializer<List<Span>> deserializer() {
        return traceDeserializer;
    }


    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        this.traceSerializer.configure(configs, isKey);
        this.traceDeserializer.configure(configs, isKey);
    }

    @Override
    public void close() {
        this.traceSerializer.close();
        this.traceDeserializer.close();
    }
}
