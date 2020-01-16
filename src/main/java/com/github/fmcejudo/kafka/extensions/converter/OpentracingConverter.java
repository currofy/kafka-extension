package com.github.fmcejudo.kafka.extensions.converter;

import com.github.fmcejudo.kafka.extensions.serialization.OpentracingDeserializer;
import com.github.fmcejudo.kafka.extensions.serialization.OpentracingSerializer;
import com.github.fmcejudo.kafka.extensions.serialization.Trace;
import org.springframework.lang.Nullable;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.converter.AbstractMessageConverter;
import org.springframework.util.Assert;
import org.springframework.util.MimeType;

public class OpentracingConverter extends AbstractMessageConverter {

    private final OpentracingDeserializer opentracingDeserializer;

    private final OpentracingSerializer opentracingSerializer;

    public OpentracingConverter() {
        super(new MimeType("application", "opentracing"));
        this.opentracingDeserializer = new OpentracingDeserializer();
        this.opentracingSerializer = new OpentracingSerializer();
    }

    @Override
    protected boolean supports(Class<?> aClass) {
        return aClass == Trace.class;
    }

    @Override
    protected Object convertFromInternal(Message<?> message, Class<?> targetClass, @Nullable Object conversionHint) {
        if (targetClass != Trace.class) {
            return null;
        }
        byte[] data = (byte[]) message.getPayload();
        return opentracingDeserializer.deserialize(null, data);
    }

    @Override
    @Nullable
    protected Object convertToInternal(
            Object payload, @Nullable MessageHeaders headers, @Nullable Object conversionHint) {

        Assert.isTrue(
                headers.get(MessageHeaders.CONTENT_TYPE).equals("application/opentracing"),
                "application/opentracing contentType header is required"
        );
        if (payload instanceof Trace) {
            return payload;
        }
        return null;
    }
}
