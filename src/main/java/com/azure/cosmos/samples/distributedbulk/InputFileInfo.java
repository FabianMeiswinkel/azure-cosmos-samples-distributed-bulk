package com.azure.cosmos.samples.distributedbulk;

import java.util.Objects;

public final class InputFileInfo {
    private final String blobName;
    private final long recordCount;
    private final long size;

    public InputFileInfo(String blobName, long size, long recordCount) {
        Objects.requireNonNull(blobName, "Argument 'blobName' must not be null.");
        this.blobName = blobName;
        this.recordCount = recordCount;
        this.size = size;
    }

    public String getBlobName() {
        return this.blobName;
    }

    public long getRecordCount() {
        return this.recordCount;
    }

    public long getSizeInBytes() {
        return this.size;
    }
}
