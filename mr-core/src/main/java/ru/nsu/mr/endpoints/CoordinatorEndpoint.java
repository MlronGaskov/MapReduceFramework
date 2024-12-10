package ru.nsu.mr.endpoints;

import com.google.gson.Gson;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import ru.nsu.mr.endpoints.dto.JobDetails;
import ru.nsu.mr.endpoints.dto.JobSummary;
import ru.nsu.mr.endpoints.dto.TaskDetails;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.function.Consumer;

public class CoordinatorEndpoint {
    private static final int STATUS_OK = 200;
    private static final int STATUS_NOT_FOUND = 404;
    private static final int STATUS_BAD_REQUEST = 400;
    private static final int STATUS_METHOD_NOT_ALLOWED = 405;

    private final Gson gson = new Gson();
    private final MetricsService metricsService;
    private final HttpServer httpServer;
    private final Consumer<String> onWorkerRegistration;
    private final Consumer<TaskDetails> onTaskNotification;

    public CoordinatorEndpoint(
            String port,
            MetricsService metricsService,
            Consumer<String> onWorkerRegistration,
            Consumer<TaskDetails> onTaskNotification)
            throws IOException {
        this.metricsService = metricsService;
        this.httpServer = HttpServer.create(new InetSocketAddress(Integer.parseInt(port)), 0);
        this.onWorkerRegistration = onWorkerRegistration;
        this.onTaskNotification = onTaskNotification;
        configureEndpoints();
    }

    private void configureEndpoints() {
        httpServer.createContext("/jobs", new JobListHandler());
        httpServer.createContext("/jobs/", new JobDetailsHandler());
        httpServer.createContext("/workers", new WorkerRegistrationHandler());
        httpServer.createContext("/notifyTask", new TaskNotificationHandler());
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
                sendErrorResponse(exchange, STATUS_METHOD_NOT_ALLOWED, "Method Not Allowed");
                return;
            }

            try (BufferedReader reader =
                    new BufferedReader(new InputStreamReader(exchange.getRequestBody()))) {
                String workerPort = gson.fromJson(reader, String.class);
                onWorkerRegistration.accept(workerPort);
                sendResponse(exchange, STATUS_OK, "Worker registered on port: " + workerPort);
            } catch (Exception e) {
                sendErrorResponse(
                        exchange,
                        STATUS_BAD_REQUEST,
                        "Failed to register worker: " + e.getMessage());
            }
        }
    }

    private class JobListHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
                sendErrorResponse(exchange, STATUS_METHOD_NOT_ALLOWED, "Method Not Allowed");
                return;
            }

            List<JobSummary> jobSummaries = metricsService.getJobs();
            sendJsonResponse(exchange, STATUS_OK, jobSummaries);
        }
    }

    private class JobDetailsHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
                sendErrorResponse(exchange, STATUS_METHOD_NOT_ALLOWED, "Method Not Allowed");
                return;
            }

            String path = exchange.getRequestURI().getPath();
            String jobId = path.substring("/jobs/".length());

            try {
                JobDetails jobDetails = metricsService.getJobDetails(jobId);
                if (jobDetails == null) {
                    sendErrorResponse(exchange, STATUS_NOT_FOUND, "Job not found for id: " + jobId);
                    return;
                }
                sendJsonResponse(exchange, STATUS_OK, jobDetails);
            } catch (IllegalArgumentException e) {
                sendErrorResponse(exchange, STATUS_BAD_REQUEST, "Invalid job ID format: " + jobId);
            } catch (Exception e) {
                sendErrorResponse(
                        exchange,
                        STATUS_BAD_REQUEST,
                        "Unable to process request: " + e.getMessage());
            }
        }
    }

    private class TaskNotificationHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
                sendErrorResponse(exchange, STATUS_METHOD_NOT_ALLOWED, "Method Not Allowed");
                return;
            }

            try (BufferedReader reader =
                    new BufferedReader(
                            new InputStreamReader(
                                    exchange.getRequestBody(), StandardCharsets.UTF_8))) {
                TaskDetails taskDetails = gson.fromJson(reader, TaskDetails.class);
                onTaskNotification.accept(taskDetails);
                sendResponse(
                        exchange,
                        STATUS_OK,
                        "Task notification received for ID: " + taskDetails.taskId());
            } catch (Exception e) {
                sendErrorResponse(
                        exchange,
                        STATUS_BAD_REQUEST,
                        "Failed to process task notification: " + e.getMessage());
            }
        }
    }

    private void sendResponse(HttpExchange exchange, int statusCode, String responseMessage)
            throws IOException {
        exchange.sendResponseHeaders(statusCode, responseMessage.getBytes().length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(responseMessage.getBytes());
        }
    }

    private void sendJsonResponse(HttpExchange exchange, int statusCode, Object responseObject)
            throws IOException {
        String jsonResponse = gson.toJson(responseObject);
        exchange.getResponseHeaders().set("Content-Type", "application/json; charset=UTF-8");
        exchange.sendResponseHeaders(
                statusCode, jsonResponse.getBytes(StandardCharsets.UTF_8).length);

        try (OutputStream os = exchange.getResponseBody()) {
            os.write(jsonResponse.getBytes(StandardCharsets.UTF_8));
        }
    }

    private void sendErrorResponse(HttpExchange exchange, int statusCode, String errorMessage)
            throws IOException {
        exchange.getResponseHeaders().set("Content-Type", "text/plain; charset=UTF-8");
        exchange.sendResponseHeaders(
                statusCode, errorMessage.getBytes(StandardCharsets.UTF_8).length);

        try (OutputStream os = exchange.getResponseBody()) {
            os.write(errorMessage.getBytes(StandardCharsets.UTF_8));
        }
    }
}
