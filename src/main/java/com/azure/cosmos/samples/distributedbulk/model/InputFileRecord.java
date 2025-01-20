package com.azure.cosmos.samples.distributedbulk.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class InputFileRecord extends StatusTracker {
    private final String blobName;

    @JsonSerialize(using = LongToStringSerializer.class)
    private final long recordCount;

    @JsonSerialize(using = LongToStringSerializer.class)
    private final long sizeInBytes;

    private final List<BatchRecord> batches;

    @JsonCreator
    public InputFileRecord(
        @JsonProperty("blobName") String blobName,
        @JsonProperty("recordCount") @JsonDeserialize(using = StringToLongDeserializer.class) Long recordCount,
        @JsonProperty("sizeInBytes") @JsonDeserialize(using = StringToLongDeserializer.class) Long sizeInBytes,
        @JsonProperty("batches") List<BatchRecord> batches) {

        Objects.requireNonNull(blobName, "Argument 'blobName' must not be null.");
        this.blobName = blobName;
        this.recordCount = recordCount != null ? recordCount : 0;
        this.sizeInBytes = sizeInBytes != null ? sizeInBytes : 0;
        this.batches = batches != null ? batches : new ArrayList<>();
    }

    public String getBlobName() {
        return this.blobName;
    }

    public long getRecordCount() {
        return this.recordCount;
    }

    public long getSizeInBytes() {
        return this.sizeInBytes;
    }

    public List<BatchRecord> getBatches() {
        return this.batches;
    }
}
