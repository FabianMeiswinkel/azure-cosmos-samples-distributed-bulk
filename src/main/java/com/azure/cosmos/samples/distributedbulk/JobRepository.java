package com.azure.cosmos.samples.distributedbulk;

import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.CosmosException;
import com.azure.cosmos.models.CosmosItemResponse;
import com.azure.cosmos.models.PartitionKey;
import com.azure.cosmos.samples.distributedbulk.model.BatchRecord;
import com.azure.cosmos.samples.distributedbulk.model.InputFileRecord;
import com.azure.cosmos.samples.distributedbulk.model.JobRecord;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class JobRepository {
    private final static ObjectMapper mapper = new ObjectMapper();
    private final static Logger logger = LoggerFactory.getLogger(JobRepository.class);
    private static final CosmosContainer jobContainer =
        Configs
            .getCosmosClient(Main.JobId + "_JobRepository")
            .getDatabase(Configs.getCosmosDatabaseName())
            .getContainer(Configs.getCosmosJobContainerName());

    public static void ensureJobDoesNotExistYet(String jobId) {
        try {
            CosmosItemResponse<ObjectNode> jobResponse = jobContainer.readItem(
                jobId,
                new PartitionKey(jobId),
                ObjectNode.class);

            throw new IllegalStateException(
                "A Job metadata document for job ID "
                    + jobId
                    + " already exists. Please use unique job ID values.");
        } catch (CosmosException cosmosException) {
            if (cosmosException.getStatusCode() == 404
                && cosmosException.getSubStatusCode() == 1003) {

                throw new IllegalStateException(
                    "The container '"
                        + Configs.getCosmosJobContainerName()
                        + "' does not exist - you have to create this container "
                        + "manually before running this sample.");
            } else if (cosmosException.getStatusCode() == 404
                && cosmosException.getSubStatusCode() == 0) {

                logger.debug(
                    "The Job metadata document for job ID {} does not exist yet - which is expected.",
                    jobId);
            } else {
                throw cosmosException;
            }
        }
    }

    public static void createNewJob(String jobId, List<InputFileInfo> inputFileInfos) {
        ensureJobDoesNotExistYet(jobId);

        List<InputFileRecord> inputFiles = new ArrayList<>();

        for (InputFileInfo inputFileInfo : inputFileInfos) {

            List<BatchRecord> batches = new ArrayList<>();
            long offset = 0;
            long index = 0;
            while (offset < inputFileInfo.getRecordCount()) {
                long recordCount = Math.min(
                    inputFileInfo.getRecordCount() - offset,
                    Configs.maxRecordsPerBatch());

                batches.add(new BatchRecord(index, recordCount, offset));
                offset += recordCount;
                index++;
            }

            inputFiles.add(new InputFileRecord(
                inputFileInfo.getBlobName(),
                inputFileInfo.getRecordCount(),
                inputFileInfo.getSizeInBytes(),
                batches));
        }

        JobRecord newJob = new JobRecord(jobId, inputFiles);

        try {
            jobContainer.createItem(newJob, new PartitionKey(jobId), null);
        } catch (CosmosException cosmosException) {
            if (cosmosException.getStatusCode() == 409
                && cosmosException.getSubStatusCode() == 1003) {

                throw new IllegalStateException(
                    "The container '"
                        + Configs.getCosmosJobContainerName()
                        + "' does not exist - you have to create this container "
                        + "manually before running this sample.");
            } else if (cosmosException.getStatusCode() == 409) {
                throw new IllegalStateException(
                    "A Job metadata document for job ID "
                        + jobId
                        + " already exists. Please use unique job ID values.");
            } else {
                throw cosmosException;
            }
        }
    }
}
