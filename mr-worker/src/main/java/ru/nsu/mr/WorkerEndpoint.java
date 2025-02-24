package ru.nsu.mr;

import com.google.gson.Gson;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import ru.nsu.mr.endpoints.dto.NewTaskDetails;
import ru.nsu.mr.endpoints.dto.TaskDetails;
import ru.nsu.mr.endpoints.dto.TaskInfo;

import java.io.*;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

public class WorkerEndpoint {
    private static final int STATUS_OK = 200;
    private static final int STATUS_BAD_REQUEST = 400;
    private static final int STATUS_NOT_FOUND = 404;
    private static final int STATUS_METHOD_NOT_ALLOWED = 405;

    private final Function<NewTaskDetails, TaskDetails> taskCreator;
    private final Function<Integer, TaskDetails> taskDetailsProvider;
    private final Supplier<List<TaskInfo>> allTasksProvider;
    private final Gson gson = new Gson();
    private HttpServer server;

    public WorkerEndpoint(
            Function<NewTaskDetails, TaskDetails> taskCreator,
            Function<Integer, TaskDetails> taskDetailsProvider,
            Supplier<List<TaskInfo>> allTasksProvider) {
        this.taskCreator = taskCreator;
        this.taskDetailsProvider = taskDetailsProvider;
        this.allTasksProvider = allTasksProvider;
    }

    public void startServer(String port) throws IOException {
        server = HttpServer.create(new InetSocketAddress(Integer.parseInt(port)), 0);
        server.createContext("/tasks", new TasksHandler());
        server.setExecutor(null);
        server.start();
    }

    public void stopServer() {
        if (server != null) {
            server.stop(0);
        }
    }

    private class TasksHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            String requestMethod = exchange.getRequestMethod();
            switch (requestMethod.toUpperCase()) {
                case "PUT":
                    handleCreateTask(exchange);
                    break;
                case "GET":
                    handleRetrieveTask(exchange);
                    break;
                default:
                    sendErrorResponse(exchange, STATUS_METHOD_NOT_ALLOWED, "Method Not Allowed");
            }
        }

        private void handleCreateTask(HttpExchange exchange) throws IOException {
            try (BufferedReader reader =
                    new BufferedReader(
                            new InputStreamReader(
                                    exchange.getRequestBody(), StandardCharsets.UTF_8))) {
                NewTaskDetails newTaskDetails = gson.fromJson(reader, NewTaskDetails.class);
                TaskDetails createdTask = taskCreator.apply(newTaskDetails);
                sendJsonResponse(exchange, STATUS_OK, createdTask);
            } catch (Exception e) {
                sendErrorResponse(
                        exchange, STATUS_BAD_REQUEST, "Failed to create task: " + e.getMessage());
            }
        }

        private void handleRetrieveTask(HttpExchange exchange) throws IOException {
            String path = exchange.getRequestURI().getPath();
            String[] pathSegments = path.split("/");

            if (pathSegments.length == 2) { // "/tasks"
                handleRetrieveAllTasks(exchange);
            } else if (pathSegments.length == 3 && !pathSegments[2].isEmpty()) { // "/tasks/{id}"
                handleRetrieveTaskById(exchange, pathSegments[2]);
            } else {
                sendErrorResponse(exchange, STATUS_BAD_REQUEST, "Invalid request path");
            }
        }

        private void handleRetrieveAllTasks(HttpExchange exchange) throws IOException {
            try {
                List<TaskInfo> tasks = allTasksProvider.get();
                sendJsonResponse(exchange, STATUS_OK, tasks);
            } catch (Exception e) {
                sendErrorResponse(
                        exchange,
                        STATUS_BAD_REQUEST,
                        "Failed to retrieve tasks: " + e.getMessage());
            }
        }

        private void handleRetrieveTaskById(HttpExchange exchange, String taskIdString)
                throws IOException {
            try {
                int taskId = Integer.parseInt(taskIdString);
                TaskDetails taskDetails = taskDetailsProvider.apply(taskId);

                if (taskDetails != null) {
                    sendJsonResponse(exchange, STATUS_OK, taskDetails);
                } else {
                    sendErrorResponse(
                            exchange, STATUS_NOT_FOUND, "Task not found for ID: " + taskId);
                }
            } catch (NumberFormatException e) {
                sendErrorResponse(
                        exchange, STATUS_BAD_REQUEST, "Invalid task ID format: " + taskIdString);
            } catch (Exception e) {
                sendErrorResponse(
                        exchange, STATUS_BAD_REQUEST, "Failed to retrieve task: " + e.getMessage());
            }
        }
    }

    private void sendJsonResponse(HttpExchange exchange, int statusCode, Object response)
            throws IOException {
        String jsonResponse = gson.toJson(response);
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
}
