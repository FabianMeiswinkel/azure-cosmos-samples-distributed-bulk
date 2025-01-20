package com.azure.cosmos.samples.distributedbulk;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.Random;

import com.azure.cosmos.samples.distributedbulk.model.BatchRecord;
import com.azure.cosmos.samples.distributedbulk.model.IngestionStatus;
import com.azure.cosmos.samples.distributedbulk.model.InputFileRecord;
import com.azure.cosmos.samples.distributedbulk.model.JobRecord;
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
                    runCore();

                    return;
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

        private void runCore() throws InterruptedException {
            Batch currentBatch = tryAcquireBatch();
            if (currentBatch == null) {
                logger.info("No incomplete batch could be acquired anymore. Retrying in 1 minute...");
                Thread.sleep(60_000);
                return;
            }

            currentBatch.run();
        }

        private BatchRecordTuple findBatchProcessingCandidate(JobRecord job) {
            for (InputFileRecord file : job.getInputFiles()) {
                for (BatchRecord batch : file.getBatches()) {

                    if (batch.getStatus() == IngestionStatus.COMPLETED) {
                        continue;
                    }

                    // acquire ownership of batch if no other worker processes the batch yet
                    // - or the worker previously acquired the batch has not updated it
                    // for at least 5 minutes
                    if (batch.getOwningWorker() == null
                        || Duration.between(batch.getOwningWorkerLastModified(), Instant.now()).toMinutes() > 5) {

                        batch.setOwningWorker(Main.getMachineId());
                        batch.setOwningWorkerLastModified(Instant.now());

                        return new BatchRecordTuple(job, file, batch);
                    }
                }
            }

            // No batch that can be acquired
            return null;
        }

        private Batch tryAcquireBatch() throws InterruptedException {

            while (true) {
                JobRecord job = JobRepository.getJobRecord(this.jobId);
                String etag = job.getEtag();
                BatchRecordTuple candidate = this.findBatchProcessingCandidate(job);

                if (candidate == null) {
                    // no batch to be processed and not acquired already
                    return null;
                }

                boolean success = JobRepository.tryUpdateJobRecord(
                    this.jobId,
                    job,
                    etag);

                if (success) {
                    return new Batch(
                        Main.getMachineId(),
                        candidate.jobRecord.getId(),
                        candidate.inputFileRecord.getBlobName(),
                        candidate.batchRecord.getIndex(),
                        candidate.batchRecord.getOffset(),
                        candidate.batchRecord.getRecordCount());
                }

                // Other worker modified job record - retry after short backoff
                Thread.sleep(10 + rnd.nextInt(1000));
            }
        }

        private static class BatchRecordTuple {
            private final JobRecord jobRecord;
            private final InputFileRecord inputFileRecord;
            private final BatchRecord batchRecord;

            public BatchRecordTuple(
                JobRecord jobRecord,
                InputFileRecord inputFileRecord,
                BatchRecord batchRecord) {

                this.jobRecord = jobRecord;
                this.inputFileRecord = inputFileRecord;
                this.batchRecord = batchRecord;
            }
        }
    }
}
