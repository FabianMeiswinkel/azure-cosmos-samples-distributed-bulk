package com.azure.cosmos.samples.distributedbulk;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class Batch implements Runnable {
    private final static Logger logger = LoggerFactory.getLogger(Batch.class);
    private final static ObjectMapper mapper = new ObjectMapper();

    private final String blobName;

    private final long offset;

    private final long recordCount;

    private final String identifier;

    public Batch(
        String jobId,
        String machineId,
        String blobName,
        int index,
        long offset,
        long recordCount) {

        Objects.requireNonNull(jobId, "Argument 'jobId' must not be null.");
        Objects.requireNonNull(machineId, "Argument 'machineId' must not be null.");
        Objects.requireNonNull(blobName, "Argument 'blobName' must not be null.");

        this.blobName = blobName;
        this.offset = offset;
        this.recordCount = recordCount;
        this.identifier = "Batch_" + blobName + "_" + index;
    }


    @Override
    public void run() {
        List<String> lines;
        File cachedFile = BlobStorage.ensureFile(this.blobName);
        try {
            BufferedReader reader = new BufferedReader(new FileReader(cachedFile.getAbsolutePath()));
            lines = reader
                .lines()
                .skip(this.offset)
                .limit(this.recordCount)
                .collect(Collectors.toList());
            reader.close();
        } catch (IOException e) {
            logger.error("Failed to read cached file '{}'", cachedFile, e);
            throw new IllegalStateException(
                "Failed to read cached file '" + cachedFile + "',",
                e);
        }

        long lineIndex = offset;
        try (BulkWriter writer = new BulkWriter(this.identifier)) {

            for (String line : lines) {
                ObjectNode doc;
                try {
                    doc = (ObjectNode) mapper.readTree(line);
                } catch (JsonProcessingException e) {
                    throw new IllegalStateException(
                        "Failed parse json of line " + lineIndex + " of file  '"
                            + cachedFile + "' - JSON ['"
                            + line + "']",
                        e);
                }

                writer.scheduleWrite(doc, lineIndex);

                lineIndex++;
            }

            logger.info("All items of batch {} scheduled.", this.identifier);
            writer.flush();
        }
    }
}
