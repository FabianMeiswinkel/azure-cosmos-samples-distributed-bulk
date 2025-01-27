package com.azure.cosmos.samples.distributedbulk.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.util.Objects;

public class InputFileRecord {
    private final String blobName;

    @JsonSerialize(using = LongToStringSerializer.class)
    private final long recordCount;

    @JsonSerialize(using = LongToStringSerializer.class)
    private final long sizeInBytes;

    @JsonSerialize(using = IntegerToStringSerializer.class)
    private final int batchCount;

    private final String id;

    private final String pk;

    @JsonProperty("recordType")
    private final String recordType = "F";

    @JsonCreator
    public InputFileRecord(
        @JsonProperty("id") String id,
        @JsonProperty("pk") String partitionKeyValue,
        @JsonProperty("blobName") String blobName,
        @JsonProperty("recordCount") @JsonDeserialize(using = StringToLongDeserializer.class) Long recordCount,
        @JsonProperty("sizeInBytes") @JsonDeserialize(using = StringToLongDeserializer.class) Long sizeInBytes,
        @JsonProperty("batchCount") @JsonDeserialize(using = StringToLongDeserializer.class) Integer batchCount) {

        Objects.requireNonNull(blobName, "Argument 'blobName' must not be null.");
        Objects.requireNonNull(id, "Argument 'id' must not be null.");
        Objects.requireNonNull(partitionKeyValue, "Argument 'partitionKeyValue' must not be null.");
        this.id = id;
        this.pk = partitionKeyValue;
        this.blobName = blobName;
        this.recordCount = recordCount != null ? recordCount : 0;
        this.sizeInBytes = sizeInBytes != null ? sizeInBytes : 0;
        this.batchCount = batchCount != null ? batchCount : 0;
    }

    public String getId() {
        return this.id;
    }

    @JsonProperty("pk")
    public String getPartitionKeyValue() {
        return this.pk;
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

    public int getBatchCount() {
        return this.batchCount;
    }

    public String getRecordType() { return this.recordType; }
}
