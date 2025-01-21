package com.azure.cosmos.samples.distributedbulk;

import com.azure.core.credential.TokenCredential;
import com.azure.cosmos.ConsistencyLevel;
import com.azure.cosmos.CosmosAsyncClient;
import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosDiagnosticsHandler;
import com.azure.cosmos.CosmosDiagnosticsThresholds;
import com.azure.cosmos.CosmosEndToEndOperationLatencyPolicyConfigBuilder;
import com.azure.cosmos.CosmosException;
import com.azure.cosmos.CosmosOperationPolicy;
import com.azure.cosmos.ThrottlingRetryOptions;
import com.azure.cosmos.models.CosmosClientTelemetryConfig;
import com.azure.cosmos.models.CosmosRequestOptions;
import com.azure.cosmos.samples.distributedbulk.model.WriteStrategy;
import com.azure.identity.DefaultAzureCredentialBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import java.util.function.Function;

public final class Configs {
    private static final Logger logger = LoggerFactory.getLogger(Configs.class);
    private static final TokenCredential credential = new DefaultAzureCredentialBuilder()
        .managedIdentityClientId(Configs.getAadManagedIdentityId())
        .authorityHost(Configs.getAadLoginUri())
        .tenantId(Configs.getAadTenantId())
        .build();

    private final static WriteStrategy writeStrategy = getWriteStrategyCore();

    private final static AtomicInteger maxConcurrentPartitionCount = new AtomicInteger(-1);

     /**
     * Returns the given string if it is nonempty; {@code null} otherwise.
     *
     * @param string the string to test and possibly return
     * @return {@code string} itself if it is nonempty; {@code null} if it is empty or null
     */
    private static String emptyToNull(String string) {
        if (string == null || string.isEmpty()) {
            return null;
        }

        return string;
    }

    public static TokenCredential getAadTokenCredential() {
        return credential;
    }

    public static String getAccountEndpoint() {
        return getRequiredConfigProperty("ACCOUNT_ENDPOINT", v -> v);
    }

    public static int getMaxConcurrentPartitionCount() {
        int snapshot = maxConcurrentPartitionCount.get();
        if (snapshot >= 1024) {
            return snapshot;
        }

        try (CosmosClient client = getCosmosClient("Configs")) {
            CosmosContainer targetContainer = client
                .getDatabase(getCosmosDatabaseName())
                .getContainer(getCosmosContainerName());

            int partitionCount = targetContainer.getFeedRanges().size();
            int targetMaxConcurrentPartitionCount = Math.max(1024, 5 * partitionCount);
            return maxConcurrentPartitionCount
                .compareAndExchange(snapshot, targetMaxConcurrentPartitionCount);

        } catch (CosmosException cosmosException) {
            logger.error(
                "Can't identify partition count of target container '{}'.",
                Configs.getCosmosContainerName());

            throw new IllegalStateException(
                "Can't identify partition count of target container '"
                    + Configs.getCosmosContainerName()
                    + "'.", cosmosException);
        }
    }

    public static int getMaxRecordsPerBatch() {
        return getOptionalConfigProperty(
            "MAX_RECORDS_PER_BATCH",
            5000,
            Integer::parseInt);
    }

    public static int getMaxRetryCount() {
        return getOptionalConfigProperty(
            "MAX_RETRY_COUNT",
            20,
            Integer::parseInt);
    }

    public static int getInitialMicroBatchSize() {
        return getOptionalConfigProperty(
            "INITIAL_MICRO_BATCH_SIZE",
            1,
            Integer::parseInt);
    }

    public static int getMaxConcurrentBatchesPerMachine() {
        return getOptionalConfigProperty(
            "MAX_CONCURRENT_BATCHES_PER_MACHINE",
            8,
            Integer::parseInt);
    }

    public static int getMaxMicroBatchSize() {
        return getOptionalConfigProperty(
            "MAX_MICRO_BATCH_SIZE",
            100,
            Integer::parseInt);
    }

    public static int getMaxMicroBatchConcurrencyPerPartition() {
        return getOptionalConfigProperty(
            "MAX_MICRO_BATCH_CONCURRENCY_PER_PARTITION",
            1,
            Integer::parseInt);
    }

    public static WriteStrategy getWriteStrategy() {
        return writeStrategy;
    }

    private static WriteStrategy getWriteStrategyCore() {
        return getOptionalConfigProperty(
            "WRITE_STRATEGY",
            WriteStrategy.UPSERT,
            WriteStrategy::fromValue);
    }

    public static String getCosmosDatabaseName() {
        return getRequiredConfigProperty("CDB_DATABASE_NAME", v -> v);
    }

    public static String getCosmosContainerName() {
        return getRequiredConfigProperty("CDB_CONTAINER_NAME", v -> v);
    }

    public static String getCosmosJobContainerName() {
        return getOptionalConfigProperty("CDB_JOB_CONTAINER_NAME", "Jobs", v -> v);
    }

    public static String getLocalBlobStoreCacheDirectory() {
        return getRequiredConfigProperty("LOCAL_BLOB_STORAGE_CACHE_DIR", v -> v);
    }

    public static String getBlobStorageAccountName() {
        return getRequiredConfigProperty("BLOB_STORAGE_ACCOUNT_NAME", v -> v);
    }

    private static CosmosClientBuilder getCosmosClientBuilder(String userAgentSuffix) {

        String effectiveUserAgentSuffix = Main.getMachineId();
        if (userAgentSuffix != null && userAgentSuffix.length() > 0) {
            effectiveUserAgentSuffix += userAgentSuffix + "_";
        }

        CosmosDiagnosticsThresholds diagnosticsThreshold = new CosmosDiagnosticsThresholds()
            .setPointOperationLatencyThreshold(Duration.ofSeconds(1))
            .setNonPointOperationLatencyThreshold(Duration.ofSeconds(2))
            .setFailureHandler((statusCode, subStatusCode) -> {
                if (statusCode < 400) {
                    return false;
                }

                if (statusCode == 404 || statusCode == 409 || statusCode == 412) {
                    return false;
                }

                if (statusCode == 429) {
                    return false;
                }

                return true;
            });
        CosmosClientTelemetryConfig telemetryConfig = new CosmosClientTelemetryConfig()
            .diagnosticsThresholds(diagnosticsThreshold)
            .diagnosticsHandler(CosmosDiagnosticsHandler.DEFAULT_LOGGING_HANDLER);

        CosmosOperationPolicy operationPolicy = cosmosOperationDetails -> {
            String resourceType = cosmosOperationDetails.getDiagnosticsContext().getResourceType();
            if ("Document".equalsIgnoreCase(resourceType)) {

                String operationType = cosmosOperationDetails.getDiagnosticsContext().getOperationType();
                if ("Batch".equalsIgnoreCase(operationType)) {
                    cosmosOperationDetails.setRequestOptions(
                        new CosmosRequestOptions()
                            .setCosmosEndToEndLatencyPolicyConfig(
                                new CosmosEndToEndOperationLatencyPolicyConfigBuilder(Duration.ofSeconds(65))
                                    .enable(true)
                                    .build()
                            )
                    );
                } else {
                    cosmosOperationDetails.setRequestOptions(
                        new CosmosRequestOptions()
                            .setCosmosEndToEndLatencyPolicyConfig(
                                new CosmosEndToEndOperationLatencyPolicyConfigBuilder(Duration.ofSeconds(10))
                                    .enable(true)
                                    .build()
                            )
                    );
                }
            }
        };

        return new CosmosClientBuilder()
            .credential(credential)
            .endpoint(getAccountEndpoint())
            .gatewayMode()
            .contentResponseOnWriteEnabled(false)
            .userAgentSuffix(effectiveUserAgentSuffix)
            .consistencyLevel(ConsistencyLevel.EVENTUAL)
            .clientTelemetryConfig(telemetryConfig)
            .addOperationPolicy(operationPolicy)
            .throttlingRetryOptions(new ThrottlingRetryOptions()
                .setMaxRetryAttemptsOnThrottledRequests(999_999)
                .setMaxRetryWaitTime(Duration.ofSeconds(65)));
    }

    public static CosmosClient getCosmosClient(String userAgentSuffix) {
        return getCosmosClientBuilder(userAgentSuffix).buildClient();
    }

    public static CosmosAsyncClient getCosmosAsyncClient(String userAgentSuffix) {
        return getCosmosClientBuilder(userAgentSuffix).buildAsyncClient();
    }

    public static String getAadLoginUri() {
        return getOptionalConfigProperty(
            "AAD_LOGIN_ENDPOINT",
            "https://login.microsoftonline.com/",
            v -> v);
    }

    public static String getAadManagedIdentityId() {
        return getOptionalConfigProperty("AAD_MANAGED_IDENTITY_ID", null, v -> v);
    }

    public static String getAadTenantId() {
        return getOptionalConfigProperty("AAD_TENANT_ID", null, v -> v);
    }

    public static String getBlobStorageContainerName() {
        return getRequiredConfigProperty("BLOB_STORAGE_CONTAINER_NAME", v -> v);
    }

    private static <T> T getOptionalConfigProperty(String name, T defaultValue, Function<String, T> conversion) {
        String textValue = getConfigPropertyOrNull(name);

        if (textValue == null) {
            return defaultValue;
        }

        T returnValue = conversion.apply(textValue);
        return returnValue != null ? returnValue : defaultValue;
    }

    private static <T> T getRequiredConfigProperty(String name, Function<String, T> conversion) {
        String textValue = getConfigPropertyOrNull(name);
        String errorMsg = "The required configuration property '"
            + name
            + "' is not specified. You can do so via system property 'COSMOS."
            + name
            + "' or environment variable 'COSMOS_" + name + "'.";
        if (textValue == null) {
            throw new IllegalStateException(errorMsg);
        }

        T returnValue = conversion.apply(textValue);
        if (returnValue == null) {
            throw new IllegalStateException(errorMsg);
        }
        return returnValue;
    }

    private static String getConfigPropertyOrNull(String name) {
        String systemPropertyName = "COSMOS." + name;
        String environmentVariableName = "COSMOS_" + name;
        String fromSystemProperty = emptyToNull(System.getProperty(systemPropertyName));
        if (fromSystemProperty != null) {
            return null;
        }

        return emptyToNull(System.getenv().get(environmentVariableName));
    }
}
