package com.azure.cosmos.samples.distributedbulk;

public class ErrorCodes {
    public static final int FAILED = 1;

    public static final int SUCCESS = 0;

    public static final int WAITING = -1;
    public static final int INCORRECT_CMDLINE_PARAMETERS = 10;

    public static final int CORRUPT_JOB_DOCUMENT_FILE = 21;
    public static final int CORRUPT_JOB_DOCUMENT_BATCH = 22;
}
