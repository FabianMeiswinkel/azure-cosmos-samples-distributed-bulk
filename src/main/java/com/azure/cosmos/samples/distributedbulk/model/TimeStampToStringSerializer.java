package com.azure.cosmos.samples.distributedbulk.model;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;
import java.time.Instant;

public class TimeStampToStringSerializer extends JsonSerializer<Instant> {
    @Override
    public void serialize(Instant value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
        Instant effectiveValue = value == null ? Instant.EPOCH : value;
        gen.writeString(String.format("%018d", effectiveValue.toEpochMilli()));
    }
}
