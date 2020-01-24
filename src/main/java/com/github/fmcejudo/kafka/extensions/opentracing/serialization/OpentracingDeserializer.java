package com.github.fmcejudo.kafka.extensions.opentracing.serialization;

import lombok.SneakyThrows;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import zipkin2.Span;
import zipkin2.SpanBytesDecoderDetector;

import java.util.List;
import java.util.Map;

public class OpentracingDeserializer implements Deserializer<List<Span>> {

    private final StringDeserializer stringDeserializer;

    public OpentracingDeserializer() {
        this.stringDeserializer = new StringDeserializer();
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        stringDeserializer.configure(configs, isKey);
    }

    @Override
    @SneakyThrows
    public List<Span> deserialize(String topic, byte[] data) {
        return decodeSpans(data);
    }

    private List<Span> decodeSpans(final byte[] data) {
        return SpanBytesDecoderDetector.decoderForListMessage(data).decodeList(data);
    }

    @Override
    public void close() {
        stringDeserializer.close();
    }
}
