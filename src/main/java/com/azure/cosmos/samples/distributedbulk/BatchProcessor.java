package com.azure.cosmos.samples.distributedbulk;

import java.time.Instant;
import java.util.Objects;
import java.util.Random;

import com.azure.cosmos.CosmosException;
import com.azure.cosmos.samples.distributedbulk.model.BatchRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BatchProcessor {
    private final static Logger logger = LoggerFactory.getLogger(BatchProcessor.class);

    private final static Random rnd = new Random();

    public static void startProcessing(String jobId, int threadCount) {
        logger.info(
            "Starting to process Job {} with {} threads...",
            jobId,
            threadCount);

        for (int i = 1; i <= threadCount; i++) {
            BatchProcessorThread runnable = new BatchProcessorThread(jobId, i);
            Thread thread = new Thread(runnable);
            thread.setDaemon(false);
            thread.setName("BatchProcessor_" + i);
            thread.setUncaughtExceptionHandler((t, e) -> {
                logger.error("Uncaught exception. {}", e.getMessage(), e);
                System.exit(ErrorCodes.FAILED);
            });
            thread.start();
        }
    }

    private static class BatchProcessorThread implements Runnable {

        private final String jobId;
        private final int threadId;

        public BatchProcessorThread(String jobId, int threadId) {
            Objects.requireNonNull(jobId, "Argument 'jobId' must not be null.");
            this.jobId = jobId;
            this.threadId = threadId;
        }

        @Override
        public void run() {
            while(true) {
                try {
                    boolean isCompleted = runCore();
                    if (isCompleted) {
                        // All batches completed
                        return;
                    }
                } catch (Exception error) {
                    logger.error(
                        "BulkProcessor thread {} of Job {} failed. Retrying in 1 minute...",
                        this.threadId,
                        this.jobId,
                        error);

                    try {
                        Thread.sleep(60_000);
                    } catch (InterruptedException e) {
                        logger.warn(
                            "Delay for thread {} of job {} was interrupted - continuing preemptively...",
                            this.threadId,
                            this.jobId,
                            e);
                    }
                }
            }
        }

        private boolean runCore() throws InterruptedException {
            Batch currentBatch = tryAcquireBatch();
            if (currentBatch == null) {
                int delayInMs = 30000 + rnd.nextInt(5_000);
                logger.info("No incomplete batch could be acquired anymore. Retrying in {}ms...", delayInMs);
                Thread.sleep(delayInMs);

                boolean isComplete = !JobRepository.hasUnfinishedBatch(jobId);
                if (isComplete) {
                    return true;
                }

                delayInMs = 10000 + rnd.nextInt(5_000);
                logger.info("Still waiting for some acquired batches to finish. Retrying in {}ms...", delayInMs);
                Thread.sleep(delayInMs);

                return false;
            }

            currentBatch.run();
            boolean isComplete = !JobRepository.hasUnfinishedBatch(jobId);
            logger.info("IsComplete after batch: {}", isComplete);
            return isComplete;
        }

        private BatchRecord findBatchProcessingCandidate(String jobId) {
            return  JobRepository.findBatchProcessingCandidate(this.threadId, jobId);
        }

        private Batch tryAcquireBatch() throws InterruptedException {
            while (true) {
                BatchRecord candidate = this.findBatchProcessingCandidate(this.jobId);

                if (candidate == null) {
                    // no batch to be processed and not acquired already
                    return null;
                }

                candidate.setOwningWorker(Main.getMachineId());
                candidate.setOwningWorkerLastModified(Instant.now());

                try {
                    BatchRecord acquiredBatchRecord = JobRepository.updateBatchRecord(candidate);
                    return new Batch(
                        this.jobId,
                        Main.getMachineId(),
                        acquiredBatchRecord.getId().split("\\|")[1],
                        acquiredBatchRecord.getIndex(),
                        acquiredBatchRecord.getOffset(),
                        acquiredBatchRecord.getRecordCount());
                } catch (CosmosException cosmosException) {
                    if (cosmosException.getStatusCode() != 412) {
                        throw cosmosException;
                    }

                    int delayInMs = 100 + 50 * rnd.nextInt(50);

                    logger.info(
                        "Conflict when trying to acquire batch '{}', Retrying to acquire another batch in {} ms.",
                        candidate.getId(),
                        delayInMs);

                    // Other worker modified job record - retry after short backoff
                    Thread.sleep(delayInMs);
                }
            }
        }
    }
}
