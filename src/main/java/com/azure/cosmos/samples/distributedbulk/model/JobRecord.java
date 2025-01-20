package com.azure.cosmos.samples.distributedbulk.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class JobRecord extends StatusTracker {
    private final String id;

    private final List<InputFileRecord> inputFiles;

    @JsonCreator
    public JobRecord(
        @JsonProperty("id") String id,
        @JsonProperty("inputFiles") List<InputFileRecord> inputFiles) {

        Objects.requireNonNull(id, "Argument 'id' must not be null.");
        this.id = id;
        this.inputFiles = inputFiles != null ? inputFiles : new ArrayList<>();
    }

    public String getId() {
        return this.id;
    }

    public List<InputFileRecord> getInputFiles() {
        return this.inputFiles;
    }
}
