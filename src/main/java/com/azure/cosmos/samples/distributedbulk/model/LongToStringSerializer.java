package com.azure.cosmos.samples.distributedbulk.model;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;

public class LongToStringSerializer extends JsonSerializer<Long> {
    @Override
    public void serialize(Long value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
        long effectiveValue = value == null ? 0 : value;
        gen.writeString(String.valueOf(effectiveValue));
    }
}
