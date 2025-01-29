package com.azure.cosmos.samples.distributedbulk;

import com.azure.cosmos.CosmosAsyncContainer;
import com.azure.cosmos.CosmosException;
import com.azure.cosmos.samples.distributedbulk.model.BatchRecord;
import com.azure.cosmos.samples.distributedbulk.model.IngestionStatus;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.time.Instant;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

public class Batch implements Runnable {
    private static final CosmosAsyncContainer container = Configs
        .getCosmosAsyncClient(Main.JobId + "_Batch")
        .getDatabase(Configs.getCosmosDatabaseName())
        .getContainer(Configs.getCosmosContainerName());

    private static final DocumentBulkExecutor<ObjectNode> bulkExecutor = new DocumentBulkExecutor<>(
        container,
        (objectNode) -> objectNode.get("id").asText()
    );
    private final static Logger logger = LoggerFactory.getLogger(Batch.class);

    private final static Random rnd = new Random();

    private final static String  BULK_WRITER_RETRY_DELAY_SCHEDULING_THREAD_NAME
        = "batchStatusUpd-";
    private final static ScheduledExecutorService statusTrackingScheduler = Executors.newScheduledThreadPool(
        1,
        new CosmosDaemonThreadFactory(BULK_WRITER_RETRY_DELAY_SCHEDULING_THREAD_NAME));

    private final String blobName;

    private final long offset;

    private final long recordCount;

    private final String jobId;

    private final int index;

    private final DocumentBulkExecutorOperationStatus status;

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

        this.jobId = jobId;
        this.blobName = blobName;
        this.offset = offset;
        this.index = index;
        this.recordCount = recordCount;
        this.status = new DocumentBulkExecutorOperationStatus("Batch_" + blobName + "_" + index);
    }


    @Override
    public void run() {
        File cachedFile = BlobStorage.ensureFile(this.blobName);
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(cachedFile.getAbsolutePath()));
            final AtomicLong lineIndex = new AtomicLong(this.offset);
            Stream<ObjectNode> docs = reader
                .lines()
                .skip(this.offset)
                .limit(this.recordCount)
                .map(line -> {
                    try {
                        lineIndex.incrementAndGet();
                        return (ObjectNode) Configs.mapper.readTree(line);
                    } catch (JsonProcessingException e) {
                        throw new IllegalStateException(
                            "Failed parse json of line " + lineIndex.get() + " of file  '"
                                + cachedFile + "' - JSON ['"
                                + line + "']",
                            e);
                    }
                })
                .filter(doc -> {
                    JsonNode opNode = doc.get("opType");
                    return opNode == null || "U".equalsIgnoreCase(opNode.asText());
                })
                .onClose(() -> logger.info("All items of batch {} scheduled.", this.status.getOperationId()));

            statusTrackingScheduler.schedule(
                this::updateStatus,
                5000 + rnd.nextInt(5000),
                TimeUnit.MILLISECONDS);

            bulkExecutor.upsertAll(
                docs,
                this.status);
        } catch (IOException e) {
            logger.error("Failed to read cached file '{}'", cachedFile, e);
            throw new IllegalStateException(
                "Failed to read cached file '" + cachedFile + "',",
                e);
        } catch (OwnershipLostException listException) {
            logger.warn(
                "Worker '{}' lost ownership of batch '{}' because another worker acquired it.",
                Main.getMachineId(),
                this.status.getOperationId());
        } finally {
            this.updateStatusLastTime();
            if (reader != null) {

                try {
                    reader.close();
                } catch (IOException e) {
                    logger.warn("Failed closing buffered reader of cached file for batch {}",
                        this.status.getOperationId());
                }
            }
        }

    }

    private void updateStatusCore(boolean isLastUpdate) {

        BatchRecord batch;
        try {
            batch = JobRepository.getBatch(this.jobId, this.blobName, this.index);
            if (!Main.getMachineId().equalsIgnoreCase(batch.getOwningWorker())
                && batch.getStatus() != IngestionStatus.COMPLETED) {

                throw new OwnershipLostException(this.jobId, this.blobName, this.index);
            }

            batch.setOwningWorkerLastModified(Instant.now());
            batch.setOwningWorker(Main.getMachineId());
            double oldestimatedProgress = batch.getEstimatedProgress();
            IngestionStatus oldStatus = batch.getStatus();
            if (batch.getRecordCount() == 0
                || batch.getStatus() == IngestionStatus.COMPLETED
                || batch.getRecordCount() <= this.status.getOperationsCompleted().get()
                || (this.status.getFlushCalled().get() && this.status.getOperationsScheduled().get() == 0)) {

                batch.setStatus(IngestionStatus.COMPLETED);
                batch.setEstimatedProgress(1d);
            } else {
                batch.setStatus(IngestionStatus.STARTED);
                batch.setEstimatedProgress(
                    (double) this.status.getOperationsCompleted().get() / (double) batch.getRecordCount());
            }

            // only renew ownership of the batch when there was any progress made
            // just allows other workers to pick-up the batch in the case the current worker
            // does not make any progress within 5 minutes
            if (oldStatus != batch.getStatus() || oldestimatedProgress != batch.getEstimatedProgress()) {
                batch = JobRepository.updateBatchRecord(batch);
            }

            if (isLastUpdate || batch.getStatus() == IngestionStatus.COMPLETED) {
                if (!JobRepository.hasUnfinishedBatch(this.jobId, this.blobName)) {
                    BlobStorage.purgeFromCache(this.blobName);

                    return;
                }
            }

            statusTrackingScheduler.schedule(
                isLastUpdate ? this::updateStatusLastTime : this::updateStatus,
                5000 + rnd.nextInt(5000),
                TimeUnit.MILLISECONDS);
        } catch (CosmosException cosmosException) {
            if (cosmosException.getStatusCode() == 412
                || cosmosException.getStatusCode() == 429
                || cosmosException.getStatusCode() == 449) {

                int delayInMs = 10000 + 50 * rnd.nextInt(100);
                logger.info(
                    "Transient error updating status for batch {} - retrying in {}ms...",
                    this.status.getOperationId(),
                    delayInMs,
                    cosmosException);

                statusTrackingScheduler.schedule(
                    isLastUpdate ? this::updateStatusLastTime : this::updateStatus,
                    delayInMs,
                    TimeUnit.MILLISECONDS);
            }
        }
    }

    private void updateStatus() {
        try {
            this.updateStatusCore(false);
        } catch (Exception error) {
            logger.error(
                "Unhandled exception trying to update status. Ignoring this optimistically.",
                error);
        }
    }

    private void updateStatusLastTime() {
        try {
            this.updateStatusCore(true);
        } catch (Exception error) {
            logger.error(
                "Unhandled exception trying to update status. Ignoring this optimistically.",
                error);
        }
    }
}
