package ru.nsu.mr.endpoints;

import com.google.gson.Gson;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import ru.nsu.mr.endpoints.data.Job;
import ru.nsu.mr.endpoints.data.JobInfo;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class CoordinatorEndpoint {
    private static final int HTTP_OK = 200;
    private static final int HTTP_NOT_FOUND = 404;
    private static final int HTTP_BAD_REQUEST = 400;
    private static final int HTTP_METHOD_NOT_ALLOWED = 405;

    private final Gson gson = new Gson();
    private final MetricsService metricsService;
    private final HttpServer server;

    public CoordinatorEndpoint(String port, MetricsService metricsService) throws IOException {
        this.metricsService = metricsService;
        this.server = HttpServer.create(new InetSocketAddress(Integer.parseInt(port)), 0);
        configureEndpoints();
    }

    private void configureEndpoints() {
        server.createContext("/jobs", new GetJobsHandler());
        server.createContext("/jobs/", new GetJobByDateHandler());
    }

    public void start() {
        server.start();
    }

    public void stop() {
        server.stop(0);
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

    private void sendErrorResponse(HttpExchange exchange, int statusCode, String message)
            throws IOException {
        exchange.getResponseHeaders().set("Content-Type", "text/plain; charset=UTF-8");
        exchange.sendResponseHeaders(statusCode, message.getBytes(StandardCharsets.UTF_8).length);

        try (OutputStream os = exchange.getResponseBody()) {
            os.write(message.getBytes(StandardCharsets.UTF_8));
        }
    }

    private class GetJobsHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
                sendErrorResponse(exchange, HTTP_METHOD_NOT_ALLOWED, "Method Not Allowed");
                return;
            }

            List<Job> jobs = metricsService.getJobs();
            sendJsonResponse(exchange, HTTP_OK, jobs);
        }
    }

    private class GetJobByDateHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
                sendErrorResponse(exchange, HTTP_METHOD_NOT_ALLOWED, "Method Not Allowed");
                return;
            }

            String path = exchange.getRequestURI().getPath();
            String dateString = path.substring("/jobs/".length());

            try {
                JobInfo jobInfo = metricsService.getJobInfo(dateString);
                if (jobInfo == null) {
                    sendErrorResponse(
                            exchange, HTTP_NOT_FOUND, "Job not found for date: " + dateString);
                    return;
                }
                sendJsonResponse(exchange, HTTP_OK, jobInfo);

            } catch (IllegalArgumentException e) {
                sendErrorResponse(exchange, HTTP_BAD_REQUEST, "Invalid date format: " + dateString);
            } catch (Exception e) {
                sendErrorResponse(
                        exchange, HTTP_BAD_REQUEST, "Unable to process request: " + e.getMessage());
            }
        }
    }
}
