package com.azure.cosmos.samples.distributedbulk;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobProperties;
import com.azure.storage.blob.models.ListBlobsOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ForkJoinPool;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class BlobStorage {
    private final static Logger logger = LoggerFactory.getLogger(BlobStorage.class);
    private final static ConcurrentHashMap<String, String> fileLocks = new ConcurrentHashMap<>();
    private final static String localCacheDirectory = Path.of(
        Configs.getLocalBlobStoreCacheDirectory(),
        Configs.getBlobStorageContainerName(),
        Main.JobId).toAbsolutePath().toString();
    private static final BlobContainerClient inputClient = new BlobServiceClientBuilder()
        .endpoint("https://" + Configs.getBlobStorageAccountName() + ".blob.core.windows.net/")
        .credential(Configs.getAadTokenCredential())
        .buildClient()
        .getBlobContainerClient(Configs.getBlobStorageContainerName());

    private static File ensureFileCore(String blobName) {
        File cacheFile = Path.of(localCacheDirectory, blobName).toFile();
        String fileLock =
            fileLocks.computeIfAbsent(cacheFile.getAbsolutePath(), fileName -> UUID.randomUUID().toString());

        synchronized (fileLock) {
            if (cacheFile.exists()) {
                logger.debug(
                    "Copy of file {} already exists in local cache {}.",
                    blobName,
                    cacheFile.getAbsolutePath());
                return cacheFile;
            }

            if (!cacheFile.getParentFile().exists() &&
                cacheFile.getParentFile().mkdirs()) {

                logger.info("Created folder for local cache - {}", cacheFile.getParent());
            }

            BlobClient blobClient = inputClient.getBlobClient(blobName);
            logger.debug(
                "Downloading file {} from Azure Blob Storage {}...",
                cacheFile.getAbsolutePath(),
                blobClient.getBlobUrl());
            Instant start = Instant.now();
            BlobProperties blob = blobClient.downloadToFile(cacheFile.getAbsolutePath(), false);
            logger.info(
                "Downloaded file {} from Azure Blob Storage {} with {} bytes successfully to local cache {} in {}ms.",
                cacheFile.getAbsolutePath(),
                blobClient.getBlobUrl(),
                blob.getBlobSize(),
                cacheFile.getAbsolutePath(),
                Duration.between(start, Instant.now()).toMillis());

            return cacheFile;
        }
    }

    public static File ensureFile(String blobName) {
        for (int i = 0; i < 10; i++) {
            if (i > 0) {
                logger.warn("RETRY {} to access file {}", i, blobName);
            }

            try {
                return ensureFileCore(blobName);
            } catch (Exception error) {
                logger.error("FAILED to access file {}.", blobName, error);
            }
        }

        throw new IllegalStateException("Can't access file '" + blobName + "'.");
    }

    public static void purgeFromCache(String blobName) {
        for (int i = 0; i < 10; i++) {
            if (i > 0) {
                logger.warn("RETRY {} to purge file {} from cache.", i, blobName);
            }

            try {
                purgeFromCacheCore(blobName);
                return;
            } catch (Exception error) {
                logger.error("FAILED to purge file {} from local cache.", blobName, error);
            }
        }

        throw new IllegalStateException("Can't access file '" + blobName + "'.");
    }

    private static void purgeFromCacheCore(String blobName) {
        File cacheFile = Path.of(localCacheDirectory, blobName).toFile();
        String fileLock =
            fileLocks.computeIfAbsent(cacheFile.getAbsolutePath(), fileName -> UUID.randomUUID().toString());

        synchronized (fileLock) {
            if (cacheFile.exists()) {
                if (cacheFile.delete()) {
                    logger.info(
                        "Cached file {} deleted from local cache {}.",
                        blobName,
                        cacheFile.getAbsolutePath());
                }
            }
        }
    }

    public static List<InputFileInfo> searchWithWildcard(String searchPatternRegex) {
        Pattern pattern = Pattern.compile(searchPatternRegex);

        List<InputFileInfo> inputFiles = Collections.synchronizedList(new ArrayList<>());

        ForkJoinPool searchThreadPool = new ForkJoinPool(
            Math.max(8, Runtime.getRuntime().availableProcessors())
        );

        List<BlobItem> blobItems = inputClient
            .listBlobs(new ListBlobsOptions(), null)
            .stream()
            .collect(Collectors.toList());

        logger.info(
            "Found {} items in blob container '{}'.",
            blobItems.size(), "https://" + Configs.getBlobStorageAccountName() + ".blob.core.windows.net/");

        searchThreadPool.submit(() ->
            blobItems
                .stream()
                .parallel()
                .forEach(blobItem -> {
                    String blobName = blobItem.getName();
                    if (pattern.matcher(blobName).matches()) {
                        File cachedFile = ensureFile(blobName);

                        try {
                            BufferedReader reader = new BufferedReader(
                                new FileReader(cachedFile.getAbsolutePath()));
                            long recordCount = reader.lines().count();
                            long size = cachedFile.length();
                            reader.close();

                            inputFiles.add(
                                new InputFileInfo(blobName, size, recordCount)
                            );
                        } catch (IOException e) {
                            logger.error("Failed to read cached file '{}'", cachedFile, e);
                        } finally {
                            purgeFromCache(blobName);
                        }
                    } else {
                        logger.debug(
                            "Skipping file '{}' because it does not match search pattern regex {}",
                            blobItem.getName(),
                            searchPatternRegex);
                    }
                })).join();

        searchThreadPool.shutdown();

        if (inputFiles.size() == 0) {
            throw new IllegalStateException(
                "No input files in the blob container "
                    + inputClient.getBlobContainerUrl()
                    + " found for search pattern regex '"
                    + searchPatternRegex
                    + "'.");
        }

        return inputFiles;
    }

    public static List<InputFileInfo> searchWithUniformWildcard(String searchPatternRegex) {
        Pattern pattern = Pattern.compile(searchPatternRegex);

        List<InputFileInfo> inputFiles = Collections.synchronizedList(new ArrayList<>());

        List<BlobItem> blobItems = inputClient
            .listBlobs(new ListBlobsOptions(), null)
            .stream()
            .collect(Collectors.toList());

        logger.info(
            "Found {} items in blob container '{}'.",
            blobItems.size(), "https://" + Configs.getBlobStorageAccountName() + ".blob.core.windows.net/");

        long recordCount = -1;
        long size = -1;

        for (BlobItem blobItem: blobItems) {
            String blobName = blobItem.getName();
            if (pattern.matcher(blobName).matches()) {
                if (recordCount < 0) {
                    File cachedFile = ensureFile(blobName);

                    try {
                        BufferedReader reader = new BufferedReader(
                            new FileReader(cachedFile.getAbsolutePath()));
                        recordCount = reader.lines().count();
                        size = cachedFile.length();
                        reader.close();
                    } catch (IOException e) {
                        logger.error("Failed to read cached file '{}'", cachedFile, e);
                    } finally {
                        purgeFromCache(blobName);
                    }
                }

                inputFiles.add(new InputFileInfo(blobName, size, recordCount));
            }
        }

        if (inputFiles.size() == 0) {
            throw new IllegalStateException(
                "No input files in the blob container "
                    + inputClient.getBlobContainerUrl()
                    + " found for search pattern regex '"
                    + searchPatternRegex
                    + "'.");
        }

        return inputFiles;
    }
}