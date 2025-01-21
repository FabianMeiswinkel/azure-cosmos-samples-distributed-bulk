package com.azure.cosmos.samples.distributedbulk;

import com.azure.cosmos.CosmosAsyncClient;
import com.azure.cosmos.CosmosAsyncContainer;
import com.azure.cosmos.CosmosEndToEndOperationLatencyPolicyConfig;
import com.azure.cosmos.CosmosEndToEndOperationLatencyPolicyConfigBuilder;
import com.azure.cosmos.CosmosException;
import com.azure.cosmos.implementation.CosmosBulkExecutionOptionsImpl;
import com.azure.cosmos.implementation.ImplementationBridgeHelpers;
import com.azure.cosmos.models.CosmosBulkExecutionOptions;
import com.azure.cosmos.models.CosmosBulkExecutionThresholdsState;
import com.azure.cosmos.models.CosmosBulkItemResponse;
import com.azure.cosmos.models.CosmosBulkOperations;
import com.azure.cosmos.models.CosmosItemOperation;
import com.azure.cosmos.models.PartitionKey;
import com.azure.cosmos.samples.distributedbulk.model.BatchRecord;
import com.azure.cosmos.samples.distributedbulk.model.IngestionStatus;
import com.azure.cosmos.samples.distributedbulk.model.InputFileRecord;
import com.azure.cosmos.samples.distributedbulk.model.JobRecord;
import com.azure.cosmos.samples.distributedbulk.model.WriteStrategy;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

class BulkWriter implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(BulkWriter.class);

    // app is using multiple bulk writer instances
    // concurrently - so, assuming one CPU per writer
    private final static int cpuCount = 1;
    private final static int maxPendingOperationCount = 1024 * 167 / cpuCount;

    private final static int maxConcurrentPartitionCount = Configs.getMaxConcurrentPartitionCount();

    // make sure we keep reference to micro batch size calculation state for
    // entire lifetime of JVM
    private final static CosmosBulkExecutionThresholdsState bulkProcessingThresholds =
        new CosmosBulkExecutionThresholdsState();

    private final static String  BULK_WRITER_INPUT_BOUNDED_ELASTIC_THREAD_NAME
        = "bwinput-";
    private final static String  BULK_WRITER_RESPONSES_BOUNDED_ELASTIC_THREAD_NAME
        = "bwrsp-";

    private final static String  BULK_WRITER_RETRY_DELAY_SCHEDULING_THREAD_NAME
        = "bwretrydelay-";
    private final static int TTL_FOR_SCHEDULER_WORKER_IN_SECONDS = 60;

    private final static Random rnd = new Random();

    // Custom bounded elastic scheduler to consume input flux
    private final Scheduler bulkWriterInputBoundedElastic = Schedulers.newBoundedElastic(
        Schedulers.DEFAULT_BOUNDED_ELASTIC_SIZE,
        Schedulers.DEFAULT_BOUNDED_ELASTIC_QUEUESIZE + 2 * maxPendingOperationCount,
        BULK_WRITER_INPUT_BOUNDED_ELASTIC_THREAD_NAME,
        TTL_FOR_SCHEDULER_WORKER_IN_SECONDS, true);

    // Custom bounded elastic scheduler to switch off IO thread to process response.
    private final Scheduler  bulkWriterResponsesBoundedElastic = Schedulers.newBoundedElastic(
        Schedulers.DEFAULT_BOUNDED_ELASTIC_SIZE,
        Schedulers.DEFAULT_BOUNDED_ELASTIC_QUEUESIZE + 2 * maxPendingOperationCount,
        BULK_WRITER_RESPONSES_BOUNDED_ELASTIC_THREAD_NAME,
        TTL_FOR_SCHEDULER_WORKER_IN_SECONDS, true);

    private final static ScheduledExecutorService retryDelayScheduler = Executors.newScheduledThreadPool(
        1,
        new CosmosDaemonThreadFactory(BULK_WRITER_RETRY_DELAY_SCHEDULING_THREAD_NAME));

    private final static ScheduledExecutorService statusTrackingScheduler = Executors.newScheduledThreadPool(
        1,
        new CosmosDaemonThreadFactory(BULK_WRITER_RETRY_DELAY_SCHEDULING_THREAD_NAME));

    private final Sinks.Many<CosmosItemOperation> bulkInputEmitter =
        Sinks.many().unicast().onBackpressureBuffer();

    private final AtomicReference<Flux<Object>> processingFluxHolder = new AtomicReference<>(null);

    //Max items to be buffered to avoid out of memory error
    private final Semaphore semaphore = new Semaphore(1024 * 167 / cpuCount);

    private static final Sinks.EmitFailureHandler emitFailureHandler =
        (signalType, emitResult) -> {
            if (emitResult.equals(Sinks.EmitResult.FAIL_NON_SERIALIZED)) {
                logger.debug("emitFailureHandler - Signal: [{}], Result: [{}]", signalType, emitResult);
                return true;
            } else if(emitResult.equals(Sinks.EmitResult.FAIL_CANCELLED)
                || emitResult.equals(Sinks.EmitResult.FAIL_TERMINATED)) {

                logger.debug(
                    "emitFailureHandlerForComplete - Already completed - Signal: [{}], Result: [{}]}",
                    signalType,
                    emitResult);
                return false;
            } else {
                logger.error("emitFailureHandler - Signal: [{}], Result: [{}]", signalType, emitResult);
                return false;
            }
        };

    private final CosmosAsyncContainer cosmosAsyncContainer;
    private final CosmosAsyncClient cosmosAsyncClient;
    private final AtomicLong operationsScheduled;
    private final AtomicLong operationsCompleted;
    private final Set<String> pendingOperations;
    private final String lockObject;

    private final String identifier;

    private final AtomicBoolean flushCalled = new AtomicBoolean(false);

    private final CosmosBulkExecutionOptions bulkOptions;

    private final Lock lock = new ReentrantLock();
    private final Condition flushCompletedCondition = lock.newCondition();

    private final String jobId;
    private String blobName;
    private final int index;

    public BulkWriter(String jobId, String blobName, int index, String identifier) {

        Objects.requireNonNull(
            jobId,
            "Argument 'jobId' must not be null.");

        Objects.requireNonNull(
            identifier,
            "Argument 'identifier' must not be null.");

        Objects.requireNonNull(
            blobName,
            "Argument 'blobName' must not be null.");

        this.jobId = jobId;
        this.blobName = blobName;
        this.index = index;
        this.cosmosAsyncClient = Configs.getCosmosAsyncClient(identifier);
        this.cosmosAsyncContainer = this.cosmosAsyncClient
            .getDatabase(Configs.getCosmosDatabaseName())
            .getContainer(Configs.getCosmosContainerName());
        this.lockObject = UUID.randomUUID().toString();
        this.identifier = identifier;
        this.operationsCompleted = new AtomicLong(0);
        this.operationsScheduled = new AtomicLong(0);
        this.pendingOperations = ConcurrentHashMap.newKeySet();

        CosmosBulkExecutionOptions options = new CosmosBulkExecutionOptions(bulkProcessingThresholds)
            .setInitialMicroBatchSize(Configs.getInitialMicroBatchSize())
            .setMaxMicroBatchSize(Configs.getMaxMicroBatchSize())
            .setMaxMicroBatchConcurrency(Configs.getMaxMicroBatchConcurrencyPerPartition());
        CosmosBulkExecutionOptionsImpl optionsImpl = ImplementationBridgeHelpers
            .CosmosBulkExecutionOptionsHelper
            .getCosmosBulkExecutionOptionsAccessor()
            .getImpl(options);

        CosmosEndToEndOperationLatencyPolicyConfig e2eTimeoutPolicy =
            new CosmosEndToEndOperationLatencyPolicyConfigBuilder(Duration.ofSeconds(65))
                .enable(true)
                .build();

        optionsImpl.setMinTargetMicroBatchSize(1);
        optionsImpl.setMaxConcurrentCosmosPartitions(maxConcurrentPartitionCount);
        optionsImpl.setCosmosEndToEndLatencyPolicyConfig(e2eTimeoutPolicy);

        this.bulkOptions = options;
    }

    @Override
    public void close() {
        Sinks.EmitResult result = Sinks.EmitResult.FAIL_TERMINATED;

        try {
            result = this.bulkInputEmitter.tryEmitError(
                new IllegalStateException("Cancelling ingestion for batch [" + this.identifier + "]."));
        } catch (Throwable t) {
            logger.info(
                "Failed to close input emitter for batch [{}}].",
                this.identifier,
                t);
        }

        logger.info(
            "Closed input emitter for batch [{}}] - {}.",
            this.identifier,
            result.name());

        try {
            this.cosmosAsyncClient.close();
        } catch (Throwable t) {
            logger.info(
                "Failed to close cosmos client for batch [{}}].",
                this.identifier,
                t);
        }

        try {
            this.bulkWriterInputBoundedElastic.disposeGracefully();
        } catch (Throwable t) {
            logger.info(
                "Failed to dispose bulkWriterInputBoundedElastic of batch [{}}].",
                this.identifier,
                t);
        }

        try {
            this.bulkWriterResponsesBoundedElastic.disposeGracefully();
        } catch (Throwable t) {
            logger.info(
                "Failed to dispose bulkWriterResponsesBoundedElastic of batch [{}}].",
                this.identifier,
                t);
        }

        this.pendingOperations.clear();
    }

    public void scheduleWrite(ObjectNode doc, long offset) {
        if (this.flushCalled.get()) {
            throw new IllegalStateException("No more writes can be scheduled after calling flush.");
        }

        Objects.requireNonNull(doc, "Argument 'doc' must not be null.");
        Objects.requireNonNull(doc.get("id"), "Argument 'doc' must have a non-null 'id' property.");

        String id = doc.get("id").asText();

        OperationContext ctx = new OperationContext(id, this.identifier, offset);

        CosmosItemOperation itemOperation;

        if (Configs.getWriteStrategy() == WriteStrategy.UPSERT) {
            itemOperation = CosmosBulkOperations.getUpsertItemOperation(
                doc,
                new PartitionKey(id),
                null,
                ctx
            );
        } else {
            itemOperation = CosmosBulkOperations.getCreateItemOperation(
                doc,
                new PartitionKey(id),
                null,
                ctx
            );
        }

        this.scheduleWrite(itemOperation);
    }

    private void scheduleRetry(CosmosItemOperation cosmosItemOperation, Throwable cause) {
        OperationContext originalCtx = cosmosItemOperation.getContext();
        int retryCount = originalCtx.getRetryCount();
        if (retryCount > Configs.getMaxRetryCount()) {
            throw new IllegalStateException(
                "Bulk ingestion of item Batch ["
                    + this.identifier
                    + "], Line [" + originalCtx.getOffset()
                    + "], ID [" + originalCtx.getId()
                    + "], PK [" + cosmosItemOperation.getPartitionKeyValue()
                    + "] failed [" + retryCount
                    + "] times. No more retries - aborting the ingestion job.", cause);
        }

        CosmosItemOperation newOperation;
        if (Configs.getWriteStrategy() == WriteStrategy.UPSERT) {
            newOperation = CosmosBulkOperations.
                getUpsertItemOperation(
                    cosmosItemOperation.getItem(),
                    new PartitionKey(originalCtx.getId()),
                    null,
                    originalCtx.createForRetry());
        } else {
            newOperation = CosmosBulkOperations.
                getCreateItemOperation(
                    cosmosItemOperation.getItem(),
                    new PartitionKey(originalCtx.getId()),
                    null,
                    originalCtx.createForRetry());
        }

        if (retryCount > 0) {
            // min 10ms per retry - max 1 second per retry
            int delayInMs = 10 * retryCount + rnd.nextInt( 990 * retryCount);
            logger.warn(
                "Item Batch {}, Line {}, Id {} failed already {} times. Retrying again in {}ms.",
                originalCtx.getIdentifier(),
                originalCtx.getOffset(),
                originalCtx.getId(),
                originalCtx.getRetryCount(),
                delayInMs);

            Runnable retrySchedulingTask = () -> scheduleInternalWrite(newOperation);

            retryDelayScheduler.schedule(retrySchedulingTask, delayInMs, TimeUnit.MILLISECONDS);
        } else {
            // retry immediately
            scheduleInternalWrite(newOperation);
        }
    }

    private void scheduleWrite(CosmosItemOperation cosmosItemOperation) {

        boolean acquired = false;
        while(!acquired) {

            try {
                acquired = semaphore.tryAcquire(1000, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                logger.warn(
                    "Thread interrupted waiting for semaphore in BulkWriter - retrying preemptively...");
            }

            if (!acquired) {
                logger.debug("Unable to acquire permit. Retrying...");
            }
        }
        logger.debug("Acquired permit");
        scheduleInternalWrite(cosmosItemOperation);
    }

    private void ensureIngestionStarted() {
        if (this.processingFluxHolder.get() == null) {
            synchronized (this.processingFluxHolder) {
                if (this.processingFluxHolder.get() == null) {

                    Flux<Object> ingestionFlux = startIngestion();

                    if (!this.processingFluxHolder.compareAndSet(null, ingestionFlux)) {
                        throw new IllegalStateException(
                            "Unexpected race condition initializing ingestion thread for " + this.identifier);
                    }

                    ingestionFlux.subscribe();

                    this.statusTrackingScheduler.schedule(
                        this::updateStatus,
                        50000 + rnd.nextInt(20000),
                        TimeUnit.MILLISECONDS);
                }
            }
        }
    }

    private void scheduleInternalWrite(CosmosItemOperation cosmosItemOperation) {
        boolean isRetry = cosmosItemOperation.<OperationContext>getContext().getRetryCount() > 0;
        if (isRetry) {
            bulkInputEmitter.emitNext(cosmosItemOperation, emitFailureHandler);
        } else {
            synchronized (this.lockObject) {
                bulkInputEmitter.emitNext(cosmosItemOperation, emitFailureHandler);
                this.operationsScheduled.incrementAndGet();
                this.pendingOperations.add(cosmosItemOperation.getId());
                this.ensureIngestionStarted();
            }
        }
    }

    public void flush() {
        Flux<Object> ingestionFluxSnapshot;
        synchronized (this.lockObject) {
            this.flushCalled.set(true);

            ingestionFluxSnapshot = this.processingFluxHolder.get();
            if (ingestionFluxSnapshot == null) {
                logger.info(
                    "Batch {} - Flush called without any writes being scheduled or ingestion started.",
                    this.identifier);

                return;
            }

            if (this.operationsScheduled.get() == 0) {
                logger.info(
                    "Batch {} - No more pending writes when flush was called. Ingested {} items.",
                    this.identifier,
                    this.operationsCompleted.get());

                return;
            }

            logger.info(
                "Batch {} - Flush called - waiting for {} pending items.",
                this.identifier,
                this.operationsScheduled.get());
        }

        Instant lastSnapshot = Instant.EPOCH;
        this.lock.lock();
        while (true) {
            boolean finished = false;
            try {
                finished = this.flushCompletedCondition
                    .await(10000, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                logger.info(
                    "Batch {} was interrupted in flush. Continuing preemptively.",
                    this.identifier,
                    e);
            }

            if (finished) {
                logger.info("Flush completed for batch [{}].", this.identifier);
                this.updateStatusLastTime();
                return;
            }

            if (Duration.between(lastSnapshot, Instant.now()).compareTo(Duration.ofMinutes(1)) > 0) {
                synchronized (this.lockObject) {
                    List<String> samples = this
                        .pendingOperations
                        .stream()
                        .limit(100)
                        .collect(Collectors.toList());

                    String sampleSet = String.join(", ", samples);

                    logger.info(
                        "Batch {} still waiting for ingestion to complete. Operations "
                            + "pending {} (Sample set {})",
                        this.identifier,
                        this.operationsScheduled,
                        sampleSet);
                }
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

    private void updateStatusCore(boolean isLastUpdate) {
        try {
            JobRecord job = JobRepository.getJobRecord(this.jobId);
            InputFileRecord file = JobRepository.findFile(job, this.blobName);
            BatchRecord batch = JobRepository.findBatch(file, this.index);

            batch.setOwningWorkerLastModified(Instant.now());
            batch.setOwningWorker(Main.getMachineId());
            if (batch.getRecordCount() == 0
                || (this.flushCalled.get() && this.operationsScheduled.get() == 0)) {
                batch.setStatus(IngestionStatus.COMPLETED);
                batch.setEstimatedProgress(1d);
            } else {
                batch.setStatus(IngestionStatus.STARTED);
                batch.setEstimatedProgress(
                    (double) this.operationsCompleted.get() / (double) batch.getRecordCount());
            }

            double totalAvgProgress = 0;
            boolean allCompleted = true;
            for (BatchRecord b : file.getBatches()) {
                totalAvgProgress += b.getEstimatedProgress();
                if (b.getStatus() != IngestionStatus.COMPLETED) {
                    allCompleted = false;
                }
            }

            if (allCompleted) {
                file.setStatus(IngestionStatus.COMPLETED);
                file.setEstimatedProgress(1d);
            } else {
                file.setStatus(IngestionStatus.STARTED);
                file.setEstimatedProgress(
                    totalAvgProgress / (double) file.getBatches().size());
            }

            totalAvgProgress = 0;
            allCompleted = true;
            for (InputFileRecord f : job.getInputFiles()) {
                totalAvgProgress += f.getEstimatedProgress();
                if (f.getStatus() != IngestionStatus.COMPLETED) {
                    allCompleted = false;
                }
            }

            if (allCompleted) {
                job.setStatus(IngestionStatus.COMPLETED);
                job.setEstimatedProgress(1d);
            } else {
                job.setStatus(IngestionStatus.STARTED);
                job.setEstimatedProgress(
                    totalAvgProgress / (double) job.getInputFiles().size());
            }

            if (JobRepository.tryUpdateJobRecord(this.jobId, job, job.getEtag())) {
                if (!isLastUpdate) {
                    int delayInMs = rnd.nextInt(50000 + rnd.nextInt(20000));
                    logger.info(
                        "Batch '{}' updated job status for jobId '{}'. Next update in {}ms.",
                        this.identifier,
                        this.jobId,
                        delayInMs);

                    statusTrackingScheduler.schedule(this::updateStatus, delayInMs, TimeUnit.MILLISECONDS);
                } else {
                    logger.info(
                        "Batch '{}' updated job status for jobId '{}' last time.",
                        this.identifier,
                        this.jobId);
                }
            } else {
                int delayInMs = rnd.nextInt(1000);
                logger.info(
                    "Conflict when batch '{}' tried to update job status for jobId '{}'. Retrying in {}ms.",
                    this.identifier,
                    this.jobId,
                    delayInMs);

                statusTrackingScheduler.schedule(
                    isLastUpdate ? this::updateStatusLastTime : this::updateStatus,
                    delayInMs,
                    TimeUnit.MILLISECONDS);
            }

        } catch (CosmosException cosmosException) {
            if (cosmosException.getStatusCode() == 412
                || cosmosException.getStatusCode() == 429
                || cosmosException.getStatusCode() == 449) {

                int delayInMs = rnd.nextInt(1000);
                logger.info(
                    "Transient error updating status - retrying in {}ms...",
                    delayInMs,
                    cosmosException);

                statusTrackingScheduler.schedule(
                    isLastUpdate ? this::updateStatusLastTime : this::updateStatus,
                    delayInMs,
                    TimeUnit.MILLISECONDS);
            }
        }
    }

    private Flux<Object> startIngestion () {
        Flux<CosmosItemOperation> inputFlux = bulkInputEmitter
            .asFlux()
            .onBackpressureBuffer()
            .publishOn(bulkWriterInputBoundedElastic)
            .doOnError(t -> logger.error("Input publishing flux failed {}", this.identifier, t));

        return cosmosAsyncContainer
            .executeBulkOperations(inputFlux, bulkOptions)
            .flatMap(bulkOperationResponse -> {
                processBulkOperationResponse(
                    bulkOperationResponse.getResponse(),
                    bulkOperationResponse.getOperation(),
                    bulkOperationResponse.getException());
                return Mono.empty();
            })
            .onBackpressureBuffer()
            .publishOn(bulkWriterResponsesBoundedElastic)
            .doOnError(t -> {
                logger.error("Bulk execution flux failed {}", this.identifier, t);
                this.lock.lock();
                try {
                    this.flushCompletedCondition.signal();
                } finally {
                    this.lock.unlock();
                }
            })
            .doOnComplete(() -> {
                logger.error("Ingestion for batch [{}] completed successfully.", this.identifier);
                this.lock.lock();
                try {
                    this.flushCompletedCondition.signal();
                } finally {
                    this.lock.unlock();
                }
            });
    }

    private void processBulkOperationResponse(
        CosmosBulkItemResponse itemResponse,
        CosmosItemOperation itemOperation,
        Exception exception) {

        if (exception != null) {
            handleException(itemOperation, exception);
        } else {
            processResponseCode(itemResponse, itemOperation);
        }
    }

    private void processResponseCode(
        CosmosBulkItemResponse itemResponse,
        CosmosItemOperation itemOperation) {

        OperationContext ctx = itemOperation.getContext();
        if (itemResponse.isSuccessStatusCode()) {
            markSuccess(ctx, itemOperation, itemResponse.getStatusCode());
        } else if (itemResponse.getStatusCode() == 409) {
            markSuccess(ctx, itemOperation, 409);
        } else if (shouldRetry(itemResponse.getStatusCode())) {
            //re-scheduling
            scheduleRetry(itemOperation, null);
        } else {
            throw new IllegalStateException(
                "Bulk ingestion of item Batch ["
                    + this.identifier
                    + "], Line [" + ctx.getOffset()
                    + "], ID [" + ctx.getId()
                    + "], PK [" + itemOperation.getPartitionKeyValue()
                    + "] failed [" + ctx.retryCount
                    + "] times. Most recent failures is not retriable.");
        }
    }

    private void handleException(CosmosItemOperation itemOperation, Exception exception) {
        OperationContext ctx = itemOperation.getContext();
        if (!(exception instanceof CosmosException)) {
            logger.error(
                "The operation for Item Batch [{}], Line [{}], ID: [{}], PK: [{}] encountered"
                    + " an unexpected failure, Retry will be attempted optimistically...",
                ctx.getIdentifier(),
                ctx.getOffset(),
                ctx.getId(),
                itemOperation.getPartitionKeyValue(),
                exception);
            scheduleRetry(itemOperation, exception);
        } else {
            CosmosException cosmosException = (CosmosException) exception;
            if (cosmosException.getStatusCode() == 409) {
                // handle as success
                this.markSuccess(ctx, itemOperation, 409);
            } else if (shouldRetry(cosmosException.getStatusCode())) {
                scheduleRetry(itemOperation, exception);
            } else  {
                throw new IllegalStateException(
                    "Bulk ingestion of item Batch ["
                        + this.identifier
                        + "], Line [" + ctx.getOffset()
                        + "], ID [" + ctx.getId()
                        + "], PK [" + itemOperation.getPartitionKeyValue()
                        + "] failed [" + ctx.retryCount
                        + "] times. Most recent failures is not retriable.", cosmosException);
            }
        }
    }

    private void markSuccess(OperationContext ctx, CosmosItemOperation itemOperation, int statusCode) {

        long scheduledCountSnapshot;
        long completedCountSnapshot;
        synchronized (this.lockObject) {
            if (!this.pendingOperations.remove(ctx.getId())) {
                logger.warn(
                    "No pending operation found for item Batch [{}], Line [{}], ID: [{}], PK: [{}]",
                    ctx.getIdentifier(),
                    ctx.getOffset(),
                    ctx.getId(),
                    itemOperation.getPartitionKeyValue());
            }

            scheduledCountSnapshot = operationsScheduled.decrementAndGet();
            completedCountSnapshot = operationsCompleted.incrementAndGet();

            if (scheduledCountSnapshot == 0 && this.flushCalled.get()) {
                this.bulkInputEmitter.emitComplete(emitFailureHandler);

                logger.info(
                    "Ingestion completed for Batch [{}] [{}] items ingested.",
                    ctx.getIdentifier(),
                    completedCountSnapshot);

                return;
            }
        }

        logger.debug(
            "The operation for Item Batch [{}], Line [{}], ID: [{}], PK: [{}] completed successfully " +
                "with a response status code: [{}]",
            ctx.getIdentifier(),
            ctx.getOffset(),
            ctx.getId(),
            itemOperation.getPartitionKeyValue(),
            statusCode);
    }

    private boolean shouldRetry(int statusCode) {
        return statusCode == 408 ||
            statusCode == 429 ||
            statusCode == 503 ||
            statusCode == 500 ||
            statusCode == 449 ||
            statusCode == 410;
    }

    private static class OperationContext {
        private final String identifier;
        private final long offset;
        private final int retryCount;

        private final String id;

        public OperationContext(
            String id,
            String identifier,
            long offset) {

            this(id, identifier, offset, 0);
        }

        private OperationContext(
            String id,
            String identifier,
            long offset,
            int retryCount) {

            Objects.requireNonNull(id, "Argument 'doc' must have a non-null 'id' property.");
            Objects.requireNonNull(identifier, "Argument 'identifier' must not be null.");
            this.id = id;
            this.identifier = identifier;
            this.offset = offset;
            this.retryCount = retryCount;
        }

        public String getId() {
            return this.id;
        }

        public String getIdentifier() {
            return this.identifier;
        }

        public long getOffset() {
            return this.offset;
        }

        public int getRetryCount() {
            return this.retryCount;
        }

        public OperationContext createForRetry() {
            return new OperationContext(this.id, this.identifier, this.offset, this.retryCount + 1);
        }
    }
}
