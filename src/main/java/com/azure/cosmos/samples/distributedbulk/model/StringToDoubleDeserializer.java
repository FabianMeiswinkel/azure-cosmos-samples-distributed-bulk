package com.azure.cosmos.samples.distributedbulk.model;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;

import java.io.IOException;

public class StringToDoubleDeserializer extends JsonDeserializer<Double> {
    @Override
    public Double deserialize(JsonParser p, DeserializationContext ctx) throws IOException {
        String value = p.getText();

        if (value == null) {
            return 0d;
        }
        try {
            return Double.parseDouble(value);
        } catch (NumberFormatException e) {
            throw new IOException("Unable to parse double value: " + value, e);
        }
    }
}
