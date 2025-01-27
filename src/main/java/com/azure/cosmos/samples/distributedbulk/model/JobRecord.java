package com.azure.cosmos.samples.distributedbulk.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class JobRecord {
    private final String id;
    private final String pk;

    private final List<String> inputFiles;

    @JsonProperty("recordType")
    private final String recordType = "J";

    @JsonCreator
    public JobRecord(
        @JsonProperty("id") String id,
        @JsonProperty("pk") String partitionKeyValue,
        @JsonProperty("inputFiles") List<String> inputFiles) {

        Objects.requireNonNull(id, "Argument 'id' must not be null.");
        Objects.requireNonNull(partitionKeyValue, "Argument 'partitionKeyValue' must not be null.");
        this.id = id;
        this.pk = partitionKeyValue;
        this.inputFiles = inputFiles != null ? inputFiles : new ArrayList<>();
    }

    public String getId() {
        return this.id;
    }

    @JsonProperty("pk")
    public String getPartitionKeyValue() {
        return this.pk;
    }

    public List<String> getInputFiles() {
        return this.inputFiles;
    }

    public String getRecordType() { return this.recordType; }
}
