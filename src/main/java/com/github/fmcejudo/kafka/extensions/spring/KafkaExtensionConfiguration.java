package com.github.fmcejudo.kafka.extensions.spring;

import com.github.fmcejudo.kafka.extensions.opentracing.converter.OpentracingConverter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.cloud.stream.annotation.StreamMessageConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.converter.MessageConverter;

@Configuration
@ConditionalOnClass(value = {
        org.springframework.cloud.stream.annotation.StreamMessageConverter.class,
        org.springframework.messaging.converter.MessageConverter.class
})
public class KafkaExtensionConfiguration {

    @Bean
    @StreamMessageConverter
    MessageConverter opentracingMessageConverter() {
        return new OpentracingConverter();
    }
}
