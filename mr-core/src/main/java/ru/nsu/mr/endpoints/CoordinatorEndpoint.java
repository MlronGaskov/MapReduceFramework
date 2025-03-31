package ru.nsu.mr.endpoints;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import ru.nsu.mr.config.Configuration;
import ru.nsu.mr.config.ConfigurationOption;
import ru.nsu.mr.endpoints.dto.NewJobDetails;
import ru.nsu.mr.endpoints.dto.TaskDetails;
import ru.nsu.mr.gateway.HttpUtils;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.function.Consumer;

public class CoordinatorEndpoint {
    private static final int STATUS_OK = 200;
    private static final int STATUS_BAD_REQUEST = 400;
    private static final int STATUS_METHOD_NOT_ALLOWED = 405;

    private final HttpServer httpServer;
    private final Consumer<String> onWorkerRegistration;
    private final Consumer<TaskDetails> onTaskNotification;
    private final Consumer<Configuration> onJobSubmission;

    public CoordinatorEndpoint(
            String coordinatorBaseUrl,
            Consumer<String> onWorkerRegistration,
            Consumer<TaskDetails> onTaskNotification,
            Consumer<Configuration> onJobSubmission) throws IOException {
        URI uri = URI.create(coordinatorBaseUrl);
        this.httpServer = HttpServer.create(new InetSocketAddress(uri.getHost(), uri.getPort()), 0);
        this.onWorkerRegistration = onWorkerRegistration;
        this.onTaskNotification = onTaskNotification;
        this.onJobSubmission = onJobSubmission;
        httpServer.createContext("/workers", new WorkerRegistrationHandler());
        httpServer.createContext("/notifyTask", new TaskNotificationHandler());
        httpServer.createContext("/job", new JobSubmissionHandler());
    }

    public void startServer() {
        httpServer.start();
    }

    public void stopServer() {
        httpServer.stop(0);
    }

    private class WorkerRegistrationHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
                HttpUtils.sendErrorResponse(exchange, STATUS_METHOD_NOT_ALLOWED, "Method Not Allowed");
                return;
            }
            try {
                String workerBaseUrl = HttpUtils.readRequestBody(exchange, String.class);
                onWorkerRegistration.accept(workerBaseUrl);
                HttpUtils.sendResponse(exchange, STATUS_OK, "Worker registered on url: " + workerBaseUrl);
            } catch (Exception e) {
                HttpUtils.sendErrorResponse(exchange, STATUS_BAD_REQUEST, "Failed to register worker: " + e.getMessage());
            }
        }
    }

    private class TaskNotificationHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
                HttpUtils.sendErrorResponse(exchange, STATUS_METHOD_NOT_ALLOWED, "Method Not Allowed");
                return;
            }
            try {
                TaskDetails taskDetails = HttpUtils.readRequestBody(exchange, TaskDetails.class);
                onTaskNotification.accept(taskDetails);
                HttpUtils.sendResponse(exchange, STATUS_OK, "Task notification received for ID: " +
                        taskDetails.taskInformation().taskId());
            } catch (Exception e) {
                HttpUtils.sendErrorResponse(exchange, STATUS_BAD_REQUEST, "Failed to process task notification: " + e.getMessage());
            }
        }
    }

    private class JobSubmissionHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!"PUT".equalsIgnoreCase(exchange.getRequestMethod())) {
                HttpUtils.sendErrorResponse(exchange, STATUS_METHOD_NOT_ALLOWED, "Method Not Allowed");
                return;
            }
            try {
                NewJobDetails jobDetails = HttpUtils.readRequestBody(exchange, NewJobDetails.class);
                Configuration jobConfig = new Configuration()
                        .set(ConfigurationOption.JOB_ID, jobDetails.jobId())
                        .set(ConfigurationOption.JOB_PATH, jobDetails.jobPath())
                        .set(ConfigurationOption.JOB_STORAGE_CONNECTION_STRING, jobDetails.jobStorageConnectionString())
                        .set(ConfigurationOption.INPUTS_PATH, jobDetails.inputsPath())
                        .set(ConfigurationOption.MAPPERS_OUTPUTS_PATH, jobDetails.mappersOutputsPath())
                        .set(ConfigurationOption.REDUCERS_OUTPUTS_PATH, jobDetails.reducersOutputsPath())
                        .set(ConfigurationOption.DATA_STORAGE_CONNECTION_STRING, jobDetails.dataStorageConnectionString())
                        .set(ConfigurationOption.MAPPERS_COUNT, jobDetails.mappersCount())
                        .set(ConfigurationOption.REDUCERS_COUNT, jobDetails.reducersCount())
                        .set(ConfigurationOption.SORTER_IN_MEMORY_RECORDS, jobDetails.sorterInMemoryRecords());
                onJobSubmission.accept(jobConfig);
                HttpUtils.sendResponse(exchange, STATUS_OK, "Job accepted");
            } catch (IllegalStateException e) {
                HttpUtils.sendErrorResponse(exchange, STATUS_BAD_REQUEST, e.getMessage());
            } catch (Exception e) {
                HttpUtils.sendErrorResponse(exchange, STATUS_BAD_REQUEST, "Failed to process job submission: " + e.getMessage());
            }
        }
    }
}
