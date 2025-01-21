package com.azure.cosmos.samples.distributedbulk.model;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;

public class IntegerToStringSerializer extends JsonSerializer<Integer> {
    @Override
    public void serialize(Integer value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
        int effectiveValue = value == null ? 0 : value;
        gen.writeString(String.valueOf(effectiveValue));
    }
}
