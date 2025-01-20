package com.azure.cosmos.samples.distributedbulk.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.time.Instant;

public class BatchRecord extends StatusTracker {
    @JsonSerialize(using = LongToStringSerializer.class)
    private final long offset;

    @JsonSerialize(using = LongToStringSerializer.class)
    private final long recordCount;

    @JsonSerialize(using = LongToStringSerializer.class)
    private final long index;

    private String owningWorker;

    @JsonDeserialize(using = StringToTimeStampDeserializer.class)
    @JsonSerialize(using = TimeStampToStringSerializer.class)
    private Instant owningWorkerAssignedAt;

    @JsonCreator
    public BatchRecord(
        @JsonProperty("index") @JsonDeserialize(using = StringToLongDeserializer.class) Long index,
        @JsonProperty("recordCount") @JsonDeserialize(using = StringToLongDeserializer.class) Long recordCount,
        @JsonProperty("offset") @JsonDeserialize(using = StringToLongDeserializer.class) Long offset) {

        this.recordCount = recordCount != null ? recordCount : 0L;
        this.index = index != null ? index : 0L;
        this.offset = offset != null ? offset : 0L;
        this.owningWorker = "";
        this.owningWorkerAssignedAt = Instant.EPOCH;
    }

    public long getOffset() {
        return this.offset;
    }

    public long getRecordCount() {
        return this.recordCount;
    }

    public String getOwningWorker() {
        return this.owningWorker;
    }

    public void setOwningWorker(String value) {
        this.owningWorker = value;
    }

    public Instant getOwningWorkerAssignedAt() {
        return this.owningWorkerAssignedAt;
    }

    public void setOwningWorkerAssignedAt(Instant value) {
        this.owningWorkerAssignedAt = value;
    }
}
