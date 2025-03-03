package ru.nsu.mr.endpoints;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
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

    public CoordinatorEndpoint(
            String coordinatorBaseUrl,
            Consumer<String> onWorkerRegistration,
            Consumer<TaskDetails> onTaskNotification) throws IOException {
        URI uri = URI.create(coordinatorBaseUrl);
        this.httpServer = HttpServer.create(new InetSocketAddress(uri.getHost(), uri.getPort()), 0);
        this.onWorkerRegistration = onWorkerRegistration;
        this.onTaskNotification = onTaskNotification;
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
}
