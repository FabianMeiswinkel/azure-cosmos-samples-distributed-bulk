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

    @JsonSerialize(using = IntegerToStringSerializer.class)
    private final int index;

    private String owningWorker;

    @JsonDeserialize(using = StringToTimeStampDeserializer.class)
    @JsonSerialize(using = TimeStampToStringSerializer.class)
    private Instant owningWorkerLastModified;

    @JsonCreator
    public BatchRecord(
        @JsonProperty("index") @JsonDeserialize(using = StringToIntegerDeserializer.class) Integer index,
        @JsonProperty("recordCount") @JsonDeserialize(using = StringToLongDeserializer.class) Long recordCount,
        @JsonProperty("offset") @JsonDeserialize(using = StringToLongDeserializer.class) Long offset) {

        this.recordCount = recordCount != null ? recordCount : 0L;
        this.index = index != null ? index : 0;
        this.offset = offset != null ? offset : 0L;
        this.owningWorker = "";
        this.owningWorkerLastModified = Instant.EPOCH;
    }

    public long getOffset() {
        return this.offset;
    }

    public long getRecordCount() {
        return this.recordCount;
    }

    public int getIndex() { return this.index; }

    public String getOwningWorker() {
        return this.owningWorker;
    }

    public void setOwningWorker(String value) {
        this.owningWorker = value;
    }

    public Instant getOwningWorkerLastModified() {
        return this.owningWorkerLastModified;
    }

    public void setOwningWorkerLastModified(Instant value) {
        this.owningWorkerLastModified = value;
    }
}
