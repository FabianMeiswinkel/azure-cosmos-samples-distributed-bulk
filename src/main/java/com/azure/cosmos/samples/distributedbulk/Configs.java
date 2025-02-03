package com.azure.cosmos.samples.distributedbulk;

import com.azure.core.credential.TokenCredential;
import com.azure.cosmos.ConnectionMode;
import com.azure.cosmos.ConsistencyLevel;
import com.azure.cosmos.CosmosAsyncClient;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosDiagnosticsHandler;
import com.azure.cosmos.CosmosDiagnosticsThresholds;
import com.azure.cosmos.CosmosEndToEndOperationLatencyPolicyConfigBuilder;
import com.azure.cosmos.CosmosOperationPolicy;
import com.azure.cosmos.DirectConnectionConfig;
import com.azure.cosmos.GatewayConnectionConfig;
import com.azure.cosmos.ThrottlingRetryOptions;
import com.azure.cosmos.models.CosmosClientTelemetryConfig;
import com.azure.cosmos.models.CosmosRequestOptions;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.Duration;

import java.util.function.Function;

public final class Configs {
    final static ObjectMapper mapper = new ObjectMapper();
    private static final TokenCredential credential = new DefaultAzureCredentialBuilder()
        .managedIdentityClientId(Configs.getAadManagedIdentityId())
        .authorityHost(Configs.getAadLoginUri())
        .tenantId(Configs.getAadTenantId())
        .build();

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

    public static String getConnectionMode() {
        return getOptionalConfigProperty(
            "CONNECTION_MODE",
            ConnectionMode.GATEWAY.toString(),
            (s) -> {
                if ("direct".equalsIgnoreCase(s)) {
                    return ConnectionMode.DIRECT.toString();
                } else {
                    return ConnectionMode.GATEWAY.toString();
                }
            });
    }

    public static int getMaxMicroBatchConcurrencyPerPartition() {
        return getOptionalConfigProperty(
            "MAX_MICRO_BATCH_CONCURRENCY_PER_PARTITION",
            1,
            Integer::parseInt);
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
            .setPointOperationLatencyThreshold(Duration.ofSeconds(30))
            .setNonPointOperationLatencyThreshold(Duration.ofSeconds(40))
            .setRequestChargeThreshold(2000)
            .setFailureHandler((statusCode, subStatusCode) -> {
                if (statusCode < 400) {
                    return false;
                }

                if (statusCode == 404 || statusCode == 409 || statusCode == 412) {
                    return false;
                }

                return statusCode != 429;
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

        if (System.getProperty("reactor.netty.tcp.sslHandshakeTimeout") == null) {
            System.setProperty("reactor.netty.tcp.sslHandshakeTimeout", "20000");
        }

        if (System.getProperty("COSMOS.HTTP_MAX_REQUEST_TIMEOUT") == null) {
            System.setProperty(
                "COSMOS.HTTP_MAX_REQUEST_TIMEOUT",
                "70");
        }

        String overrideJson = "{\"timeoutDetectionEnabled\": true, \"timeoutDetectionDisableCPUThreshold\": 75.0," +
            "\"timeoutDetectionTimeLimit\": \"PT90S\", \"timeoutDetectionHighFrequencyThreshold\": 10," +
            "\"timeoutDetectionHighFrequencyTimeLimit\": \"PT30S\", \"timeoutDetectionOnWriteThreshold\": 10," +
            "\"timeoutDetectionOnWriteTimeLimit\": \"PT90s\", \"tcpNetworkRequestTimeout\": \"PT7S\", " +
            "\"connectTimeout\": \"PT10S\", \"maxChannelsPerEndpoint\": \"130\"}";

        if (System.getProperty("reactor.netty.tcp.sslHandshakeTimeout") == null) {
            System.setProperty("reactor.netty.tcp.sslHandshakeTimeout", "20000");
        }

        if (System.getProperty("COSMOS.HTTP_MAX_REQUEST_TIMEOUT") == null) {
            System.setProperty(
                "COSMOS.HTTP_MAX_REQUEST_TIMEOUT",
                "70");
        }

        if (System.getProperty("COSMOS.DEFAULT_HTTP_CONNECTION_POOL_SIZE") == null) {
            System.setProperty(
                "COSMOS.DEFAULT_HTTP_CONNECTION_POOL_SIZE",
                "25000");
        }

        if (System.getProperty("azure.cosmos.directTcp.defaultOptions") == null) {
            System.setProperty("azure.cosmos.directTcp.defaultOptions", overrideJson);
        }

        CosmosClientBuilder builder = new CosmosClientBuilder()
            .credential(credential)
            .endpoint(getAccountEndpoint());

        if (ConnectionMode.DIRECT.toString().equals(getConnectionMode())) {
            GatewayConnectionConfig gwConfig = new GatewayConnectionConfig()
                .setIdleConnectionTimeout(Duration.ofSeconds(70))
                .setMaxConnectionPoolSize(10000);
            DirectConnectionConfig directConfig = new DirectConnectionConfig()
                .setConnectTimeout(Duration.ofSeconds(10))
                .setNetworkRequestTimeout(Duration.ofSeconds(10));
            builder = builder.directMode(directConfig, gwConfig);
        } else {
            GatewayConnectionConfig gwConfig = new GatewayConnectionConfig()
                .setIdleConnectionTimeout(Duration.ofSeconds(70))
                .setMaxConnectionPoolSize(25000);
            builder = builder.gatewayMode(gwConfig);
        }

        return builder
            .contentResponseOnWriteEnabled(false)
            .userAgentSuffix(effectiveUserAgentSuffix)
            .consistencyLevel(ConsistencyLevel.SESSION)
            .clientTelemetryConfig(telemetryConfig)
            .addOperationPolicy(operationPolicy)
            .throttlingRetryOptions(new ThrottlingRetryOptions()
                .setMaxRetryAttemptsOnThrottledRequests(999_999)
                .setMaxRetryWaitTime(Duration.ofSeconds(65)));
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
            return fromSystemProperty;
        }

        return emptyToNull(System.getenv().get(environmentVariableName));
    }
}
