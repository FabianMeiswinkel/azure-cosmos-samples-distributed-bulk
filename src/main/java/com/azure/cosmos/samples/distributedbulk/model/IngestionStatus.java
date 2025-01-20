package com.azure.cosmos.samples.distributedbulk.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public enum IngestionStatus {
        NONE("none"),
        STARTED("started"),
        COMPLETED("completed");

        private final String value;

        IngestionStatus(String value) {
            this.value = value;
        }

        @JsonValue
        public String getValue() {
            return value;
        }

        @JsonCreator
        public static IngestionStatus fromValue(String value) {
            for (IngestionStatus status : IngestionStatus.values()) {
                if (status.value.equalsIgnoreCase(value)) {
                    return status;
                }
            }

            throw new IllegalArgumentException("Unknown enum value: " + value);
        }
}
