package com.azure.cosmos.samples.distributedbulk.model;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

public abstract class  StatusTracker {
    private IngestionStatus status;

    @JsonDeserialize(using = StringToDoubleDeserializer.class)
    @JsonSerialize(using = DoubleToStringSerializer.class)
    private double estimatedProgress;

    protected StatusTracker() {
        this.status = IngestionStatus.NONE;
        this.estimatedProgress = 0d;
    }

    public double getEstimatedProgress() {
        return this.estimatedProgress;
    }

    public void setEstimatedProgress(double value) {
        this.estimatedProgress = value;
    }

    public IngestionStatus getStatus() {
        return this.status;
    }

    public void setStatus(IngestionStatus value) {
        this.status = value != null ? value : IngestionStatus.NONE;
    }
}
