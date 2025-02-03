package com.azure.cosmos.samples.distributedbulk;

import com.azure.cosmos.CosmosAsyncContainer;
import com.azure.cosmos.models.CosmosBulkOperations;
import com.azure.cosmos.models.CosmosItemOperation;
import com.azure.cosmos.models.PartitionKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Stream;

public class DocumentBulkExecutor<T> {
    private final static Logger logger = LoggerFactory.getLogger(DocumentBulkExecutor.class);
    private final CosmosAsyncContainer cosmosAsyncContainer;
    private final Function<T, String> idExtractor;

    public DocumentBulkExecutor(
        CosmosAsyncContainer cosmosAsyncContainer,
        Function<T, String> idExtractor) {

        Objects.requireNonNull(cosmosAsyncContainer, "Argument 'cosmosAsyncContainer' must not be null.");
        Objects.requireNonNull(idExtractor, "Argument 'idExtractor' must not be null.");
        this.cosmosAsyncContainer = cosmosAsyncContainer;
        this.idExtractor = idExtractor;
    }

    /*
    public BulkDeleteResponse deleteAll(List<Pair<String, String>> pkIdPairsToDelete) {

    }

    public BulkImportResponse importAll(Collection<?> documents) {
    }*/

    /**
     * Upserts all documents in bulk mode
     * @param documents a collection of documents for a logical batch
     * @return A BulkImportResponse with aggregated diagnostics and eventual failures.
     */
    public BulkImportResponse upsertAll(Stream<T> documents) {
        return this.upsertAll(
            documents,
            new DocumentBulkExecutorOperationStatus());
    }

    /**
     * Upserts all documents in bulk mode
     * @param documents a collection of documents for a logical batch
     * @param status can be used to track the status of the bulk ingestion for this batch at real-time
     * @return A BulkImportResponse with aggregated diagnostics and eventual failures.
     */
    public BulkImportResponse upsertAll(
        Stream<T> documents,
        DocumentBulkExecutorOperationStatus status) {

        Objects.requireNonNull(documents, "Argument 'documents' must not be null.");
        Objects.requireNonNull(status, "Argument 'status' must not be null.");

        try {
            Stream<CosmosItemOperation> docsToUpsert = documents
                .map(doc -> {
                        String id = this.idExtractor.apply(doc);
                        return CosmosBulkOperations.getUpsertItemOperation(
                                doc,
                                new PartitionKey(id),
                                null,
                                new OperationContext(id, status.getOperationId()));
                    });
            return executeBulkOperations(docsToUpsert, status);
        } catch (Throwable t) {
            logger.error("Failed to upsert documents - {}", t.getMessage(), t);
            throw new IllegalStateException("Failed to upsert documents", t);
        }
    }

    private BulkImportResponse executeBulkOperations(
        Stream<CosmosItemOperation> operations,
        DocumentBulkExecutorOperationStatus status) {

        Instant startTime = Instant.now();
        try (BulkWriter writer = new BulkWriter(
            this.cosmosAsyncContainer,
            status)) {

            operations
                .forEach(writer::scheduleWrite);

            logger.info("All items of batch {} scheduled.", status.getOperationId());
            writer.flush();

            List<Object> badInputDocs = status.getBadInputDocumentsSnapshot();
            List<BulkImportFailure> failures = status.getFailuresSnapshot();

            logger.info(
                "Completed {}. Duration: {}, Ingested {} docs, Total RU: {}, Bad Documents: {}, Failures: {}.",
                status.getOperationId(),
                Duration.between(status.getStartedAt(), Instant.now()).toString(),
                status.getOperationsCompleted().get(),
                status.getTotalRequestChargeSnapshot(),
                badInputDocs != null ? badInputDocs.size() : 0,
                failures != null ? failures.size() : 0);

            return new BulkImportResponse(
                (int)status.getOperationsCompleted().get(),
                status.getTotalRequestChargeSnapshot(),
                Duration.between(startTime, Instant.now()),
                null,
                badInputDocs,
                failures);
        }
    }
}
