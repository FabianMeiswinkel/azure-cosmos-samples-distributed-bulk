package com.azure.cosmos.samples.distributedbulk;

import com.azure.cosmos.CosmosAsyncContainer;
import com.azure.cosmos.CosmosDiagnosticsThresholds;
import com.azure.cosmos.CosmosException;
import com.azure.cosmos.models.CosmosItemRequestOptions;
import com.azure.cosmos.models.CosmosItemResponse;
import com.azure.cosmos.models.CosmosQueryRequestOptions;
import com.azure.cosmos.models.PartitionKey;
import com.azure.cosmos.models.SqlParameter;
import com.azure.cosmos.models.SqlQuerySpec;
import com.azure.cosmos.samples.distributedbulk.model.BatchRecord;
import com.azure.cosmos.samples.distributedbulk.model.InputFileRecord;
import com.azure.cosmos.samples.distributedbulk.model.JobRecord;
import com.azure.cosmos.util.CosmosPagedFlux;
import com.azure.cosmos.util.CosmosPagedIterable;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class JobRepository {
    private final static Logger logger = LoggerFactory.getLogger(JobRepository.class);

    private static final CosmosAsyncContainer jobContainer =
        Configs
            .getCosmosAsyncClient(Main.JobId + "_JobRepository")
            .getDatabase(Configs.getCosmosDatabaseName())
            .getContainer(Configs.getCosmosJobContainerName());

    public static void ensureJobDoesNotExistYet(String jobId) {
        try {
            jobContainer.readItem(
                jobId,
                new PartitionKey(jobId),
                ObjectNode.class).block();

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
        JobRecord jobRecord = jobContainer.readItem(
                jobId,
                new PartitionKey(jobId),
                JobRecord.class).block().getItem();

        for (String file : jobRecord.getInputFiles()) {
            BlobStorage.purgeFromCache(file);
        }

        return jobRecord;
    }

    public static BatchRecord findBatchProcessingCandidate(String jobId) {
        long ownerShipExpiration = Instant.now().minus(5, ChronoUnit.MINUTES).toEpochMilli();
        CosmosQueryRequestOptions queryOptions = new CosmosQueryRequestOptions()
            .setPartitionKey(new PartitionKey(jobId))
            .setQueryName("FindBatchCandidate")
            .setDiagnosticsThresholds(new CosmosDiagnosticsThresholds()
                .setNonPointOperationLatencyThreshold(Duration.ofMillis(1000)));

        List<SqlParameter> parameters = new ArrayList<>();
        parameters.add(new SqlParameter("@OwnershipExpiration", ownerShipExpiration));

        SqlQuerySpec query = new SqlQuerySpec()
            .setQueryText("SELECT * FROM c WHERE c.recordType = \"B\" AND c.status != \"COMPLETED\" AND (c.owningWorkerLastModified < @OwnershipExpiration OR IS_NULL(c.owningWorker) OR c.owningWorker=\"\")")
            .setParameters(parameters);

        logger.info("Executing query {} - {} - {}: {}",
            "FindBatchCandidate",
            query.getQueryText(),
            parameters.get(0).getName(),
            parameters.get(0).getValue(Long.class));
        CosmosPagedFlux<BatchRecord> batchPagedFlux = jobContainer.queryItems(
            query,
            queryOptions,
            BatchRecord.class
        );

        CosmosPagedIterable<BatchRecord> batchPagedIterable = new CosmosPagedIterable<>(
            batchPagedFlux,
            1
        );

        Iterator<BatchRecord> candidatesIterator = batchPagedIterable.iterator();

        if (candidatesIterator.hasNext()) {
            return candidatesIterator.next();
        }

        return null;
    }

    public static boolean hasUnfinishedBatch(String jobId, String blobName) {
        CosmosQueryRequestOptions queryOptions = new CosmosQueryRequestOptions()
            .setPartitionKey(new PartitionKey(jobId))
            .setQueryName("FindUnfinishedBatch")
            .setDiagnosticsThresholds(new CosmosDiagnosticsThresholds()
                .setNonPointOperationLatencyThreshold(Duration.ofMillis(1000)));

        List<SqlParameter> parameters = new ArrayList<>();
        parameters.add(new SqlParameter("@IdPrefix", String.join("|", jobId, blobName)));

        SqlQuerySpec query = new SqlQuerySpec()
            .setQueryText("SELECT c.id FROM c WHERE c.recordType = \"B\" AND c.status != \"COMPLETED\" AND STARTSWITH(c.id, @IdPrefix)")
            .setParameters(parameters);
        logger.info("Executing query {} - {} - {}: {}",
            "FindUnfinishedBatch",
            query.getQueryText(),
            parameters.get(0).getName(),
            parameters.get(0).getValue(String.class));
        CosmosPagedFlux<ObjectNode> batchPagedFlux = jobContainer.queryItems(
            query,
            queryOptions,
            ObjectNode.class
        );

        CosmosPagedIterable<ObjectNode> batchPagedIterable = new CosmosPagedIterable<>(
            batchPagedFlux,
            1
        );

        Iterator<ObjectNode> candidatesIterator = batchPagedIterable.iterator();

        return candidatesIterator.hasNext();
    }

    public static BatchRecord updateBatchRecord(BatchRecord batch) {
        CosmosItemRequestOptions options = new CosmosItemRequestOptions()
            .setIfMatchETag(batch.getEtag());

        CosmosItemResponse<BatchRecord> batchResponse = jobContainer.replaceItem(
            batch,
            batch.getId(),
            new PartitionKey(batch.getPartitionKeyValue()),
            options
        ).block();

        batch.setEtag(batchResponse.getETag());

        return batch;
    }

    public static BatchRecord getBatch(String jobId, String blobName, int index) {
        String batchId = String.join("|", jobId, blobName, String.valueOf(index));

        return jobContainer.readItem(
            batchId,
            new PartitionKey(jobId),
            BatchRecord.class).block().getItem();
    }

    public static void createNewJob(String jobId, List<InputFileInfo> inputFileInfos) {
        ensureJobDoesNotExistYet(jobId);

        List<String> inputFiles = new ArrayList<>();
        List<InputFileRecord> inputFileRecordsToBeUpserted = new ArrayList<>();
        List<BatchRecord> batchRecordsToBeUpserted = new ArrayList<>();

        for (InputFileInfo inputFileInfo : inputFileInfos) {

            long offset = 0;
            int index = 1;
            while (offset < inputFileInfo.getRecordCount()) {
                long recordCount = Math.min(
                    inputFileInfo.getRecordCount() - offset,
                    Configs.getMaxRecordsPerBatch());

                String batchId = jobId + "|" + inputFileInfo.getBlobName() + "|" + index;
                batchRecordsToBeUpserted.add(new BatchRecord(batchId, jobId,  index, recordCount, offset));
                offset += recordCount;
                index++;
            }

            String fileId = jobId + "|" + inputFileInfo.getBlobName();

            inputFileRecordsToBeUpserted.add(new InputFileRecord(
                fileId,
                jobId,
                inputFileInfo.getBlobName(),
                inputFileInfo.getRecordCount(),
                inputFileInfo.getSizeInBytes(),
                index - 1));

            inputFiles.add(inputFileInfo.getBlobName());
        }

        JobRecord newJob = new JobRecord(jobId, jobId, inputFiles);
        createJobCore(newJob, jobId);
        for (InputFileRecord newFile : inputFileRecordsToBeUpserted) {
            jobContainer.upsertItem(newFile, new PartitionKey(jobId), null).block();
        }

        for (BatchRecord newBatch : batchRecordsToBeUpserted) {
            jobContainer.upsertItem(newBatch, new PartitionKey(jobId), null).block();
        }
    }

    private static void createJobCore(JobRecord newJob, String jobId) {
        try {
            jobContainer.createItem(newJob, new PartitionKey(jobId), null).block();
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
