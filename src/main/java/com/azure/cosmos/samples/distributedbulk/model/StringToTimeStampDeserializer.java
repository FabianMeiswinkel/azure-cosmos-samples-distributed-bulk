package com.azure.cosmos.samples.distributedbulk.model;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;

import java.io.IOException;
import java.time.Instant;

public class StringToTimeStampDeserializer extends JsonDeserializer<Instant> {
    @Override
    public Instant deserialize(JsonParser p, DeserializationContext ctx) throws IOException {
        String value = p.getText();

        if (value == null) {
            return Instant.EPOCH;
        }
        try {
            return Instant.ofEpochMilli(Long.parseLong(value));
        } catch (NumberFormatException e) {
            throw new IOException("Unable to parse timestamp (in epoch ms) value: " + value, e);
        }
    }
}
