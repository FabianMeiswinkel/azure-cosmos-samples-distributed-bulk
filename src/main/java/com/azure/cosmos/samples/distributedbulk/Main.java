package com.azure.cosmos.samples.distributedbulk;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.URL;
import java.util.List;
import java.util.UUID;

public class Main {
    private final static Logger logger = LoggerFactory.getLogger(Main.class);

    public static String JobId;

    private static String machineId;

    public static String getMachineId() {
        return machineId;
    };

    private static String computeMachineId(String[] args) {
        String prefix = "";



        String suffix = args != null && args.length > 1 ? args[0] : "";

        logger.info("Trying to read VM metadata from IMDS endpoint to extract VMId...");
        try {
            URL url = new URL("http://169.254.169.254/metadata/instance?api-version=2021-02-01");
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            conn.setRequestProperty("Metadata", "true");

            BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
            String inputLine;
            StringBuilder content = new StringBuilder();
            while ((inputLine = in.readLine()) != null) {
                content.append(inputLine);
            }

            // Close connections
            in.close();
            conn.disconnect();

            // Parse the VM metdata to extract the VMId
            ObjectMapper mapper = new ObjectMapper();
            ObjectNode parsedVmMetadata = (ObjectNode) mapper.readTree(content.toString());

            prefix = parsedVmMetadata.get("compute").get("name").asText() + "_";
            String vmId = parsedVmMetadata.get("compute").get("vmId").asText();

            if (suffix.length() > 0) {
                return prefix + "vmId-" + vmId + "_" + suffix + "_";
            }

            return prefix + "vmId-" + vmId + "_";

        } catch (Exception e) {
            String uuid = UUID.randomUUID().toString();
            logger.warn(
                "Failed to read VMId from IMDS endpoint - using an artificial uuid {} instead.",
                uuid);

            try {
                prefix = InetAddress.getLocalHost().getHostName() + "_";
            } catch (Throwable error) {
                logger.warn("Could not identify machine name", error);
            }

            if (suffix.length() > 0) {
                return prefix + "uuid-" + uuid + "_" + suffix + "_";
            }

            return prefix + "uuid-" + uuid + "_";
        }
    }

    public static void main(String[] args) {
        logger.info("Command arguments: {}", String.join(", ", args));

        machineId = computeMachineId(args);
        logger.info("MachineId: {}", machineId);

        try {
            if (args.length == 3 && "create".equalsIgnoreCase(args[0])) {
                JobId = args[1];
                System.exit(createJob(args[1], args[2]));
                return;
            } else if (args.length == 2 && "monitor".equalsIgnoreCase(args[0])) {
                JobId = args[1];
            } else if (args.length == 2 && "process".equalsIgnoreCase(args[0])) {
                JobId = args[1];
            } else {
                printHelp(args);
                System.exit(ErrorCodes.INCORRECT_CMDLINE_PARAMETERS);
                return;
            }
        } catch (Throwable error) {
            logger.error("FAILURE: {}", error);
            System.exit((ErrorCodes.FAILED));
            return;
        }

        System.exit(ErrorCodes.SUCCESS);
    }

    private static void printHelp(String[] args) {
        String msg = "Invalid command line parameters '"
            + String.join(", ", args)
            + "'. Valid commands are";

        System.out.println(msg);
        logger.error(msg);

        System.out.println("    create <JobID> <InputFileSearchPattern>");
        logger.error("    create <JobID> <InputFileSearchPattern>");

        System.out.println("    monitor <JobID>");
        logger.error("    monitor <JobID>");

        System.out.println("    process <JobID>");
        logger.error("    process <JobID>");
    }

    private static int createJob(
        String jobId,
        String inputFileSearchPattern) {

        try {
            JobRepository.ensureJobDoesNotExistYet(jobId);

            List<InputFileInfo> inputFiles =
                BlobStorage.searchWithWildcard(inputFileSearchPattern);

            JobRepository.createNewJob(jobId, inputFiles);

            return ErrorCodes.SUCCESS;
        } catch (Exception error) {
            logger.error(
                "Attempt to create job with ID {} and input file search pattern {} failed.",
                jobId,
                inputFileSearchPattern,
                error);

            return ErrorCodes.FAILED;
        }
    }
}
