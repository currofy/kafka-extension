package com.github.fmcejudo.kafka.extensions.serialization;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import zipkin2.Span;

import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Trace {

    private String traceId;

    private List<Span> spans;
}
