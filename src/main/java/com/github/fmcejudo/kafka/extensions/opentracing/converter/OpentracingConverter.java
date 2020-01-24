package com.github.fmcejudo.kafka.extensions.opentracing.converter;

import org.springframework.lang.Nullable;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.converter.AbstractMessageConverter;
import org.springframework.util.Assert;
import org.springframework.util.MimeType;
import zipkin2.Span;
import zipkin2.SpanBytesDecoderDetector;

import java.util.List;

public class OpentracingConverter extends AbstractMessageConverter {

    public OpentracingConverter() {
        super(new MimeType("application", "opentracing"));
    }

    @Override
    protected boolean supports(Class<?> aClass) {
        return aClass == List.class;
    }

    @Override
    protected List<Span> convertFromInternal(Message<?> message, Class<?> targetClass, @Nullable Object conversionHint) {
        if (targetClass != List.class) {
            return null;
        }
        byte[] data = (byte[]) message.getPayload();
        return SpanBytesDecoderDetector.decoderForListMessage(data).decodeList(data);
    }

    @Override
    @Nullable
    protected List<Span> convertToInternal(
            Object payload, @Nullable MessageHeaders headers, @Nullable Object conversionHint) {

        Assert.isTrue(
                headers.get(MessageHeaders.CONTENT_TYPE).equals("application/opentracing"),
                "application/opentracing contentType header is required"
        );
        if (payload instanceof List) {
            return (List<Span>) payload;
        }
        return null;
    }
}
