package com.azure.cosmos.samples.distributedbulk.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public enum WriteStrategy {
    INSERT_IF_NOT_EXISTS("insert_if_not_exists"),
    UPSERT("upsert");

    private final String value;

    WriteStrategy(String value) {
        this.value = value;
    }

    @JsonValue
    public String getValue() {
        return value;
    }

    @JsonCreator
    public static WriteStrategy fromValue(String value) {
        for (WriteStrategy strategy : WriteStrategy.values()) {
            if (strategy.value.equalsIgnoreCase(value)) {
                return strategy;
            }
        }

        throw new IllegalArgumentException("Unknown enum value: " + value);
    }
}

