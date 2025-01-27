package com.azure.cosmos.samples.distributedbulk;

public class OwnershipLostException extends RuntimeException {
    public OwnershipLostException(String jobId, String blobName, int batchIndex) {
        super("Lost ownership of batch '" + jobId + "|" + blobName + "|" + batchIndex + ".");
    }
}
