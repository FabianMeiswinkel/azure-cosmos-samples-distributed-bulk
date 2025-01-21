package com.azure.cosmos.samples.distributedbulk;

import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.CosmosException;
import com.azure.cosmos.models.CosmosItemRequestOptions;
import com.azure.cosmos.models.PartitionKey;
import com.azure.cosmos.samples.distributedbulk.model.BatchRecord;
import com.azure.cosmos.samples.distributedbulk.model.InputFileRecord;
import com.azure.cosmos.samples.distributedbulk.model.JobRecord;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class JobRepository {
    private final static Logger logger = LoggerFactory.getLogger(JobRepository.class);
    private static final CosmosContainer jobContainer =
        Configs
            .getCosmosClient(Main.JobId + "_JobRepository")
            .getDatabase(Configs.getCosmosDatabaseName())
            .getContainer(Configs.getCosmosJobContainerName());

    public static void ensureJobDoesNotExistYet(String jobId) {
        try {
            jobContainer.readItem(
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

    public static JobRecord getJobRecord(String jobId) {
        return jobContainer.readItem(
                jobId,
                new PartitionKey(jobId),
                JobRecord.class).getItem();
    }

    public static boolean tryUpdateJobRecord(String jobId, JobRecord jobRecord, String etag) {
        try {
            CosmosItemRequestOptions requestOptions = new CosmosItemRequestOptions();

            requestOptions.setIfMatchETag(etag);

            jobContainer.replaceItem(
                jobRecord,
                jobId,
                new PartitionKey(jobId),
                requestOptions);

            return true;
        }
        catch (CosmosException cosmosException) {
            if (cosmosException.getStatusCode() == 412) {
                return false;
            }

            throw cosmosException;
        }
    }

    public static InputFileRecord findFile(JobRecord job, String blobName) {
        List<InputFileRecord> files = job.getInputFiles()
                                         .stream()
                                         .filter(f -> blobName.equals(f.getBlobName()))
                                         .collect(Collectors.toList());
        if (files.size() != 1) {
            logger.error(
                "Expected to find exactly one file with name '{}}'.",
                blobName);

            System.exit(ErrorCodes.CORRUPT_JOB_DOCUMENT_FILE);
        }

        return files.get(0);
    }

    public static BatchRecord findBatch(InputFileRecord file, int index) {
        List<BatchRecord> batches = file.getBatches()
                                         .stream()
                                         .filter(b -> index == b.getIndex())
                                         .collect(Collectors.toList());
        if (batches.size() != 1) {
            logger.error(
                "Expected to find exactly one batch with index '{}}' in file '{}'.",
                index,
                file.getBlobName());

            System.exit(ErrorCodes.CORRUPT_JOB_DOCUMENT_BATCH);
        }

        return batches.get(0);
    }

    public static void createNewJob(String jobId, List<InputFileInfo> inputFileInfos) {
        ensureJobDoesNotExistYet(jobId);

        List<InputFileRecord> inputFiles = new ArrayList<>();

        for (InputFileInfo inputFileInfo : inputFileInfos) {

            List<BatchRecord> batches = new ArrayList<>();
            long offset = 0;
            int index = 0;
            while (offset < inputFileInfo.getRecordCount()) {
                long recordCount = Math.min(
                    inputFileInfo.getRecordCount() - offset,
                    Configs.getMaxRecordsPerBatch());

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

        JobRecord newJob = new JobRecord(jobId, null, inputFiles);

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
