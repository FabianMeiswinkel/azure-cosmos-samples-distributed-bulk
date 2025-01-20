package com.azure.cosmos.samples.distributedbulk.model;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;

public class DoubleToStringSerializer extends JsonSerializer<Double> {
    @Override
    public void serialize(Double value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
        double effectiveValue = value == null ? 0 : value;
        gen.writeString(String.valueOf(effectiveValue));
    }
}
