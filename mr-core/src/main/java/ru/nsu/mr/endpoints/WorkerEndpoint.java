package ru.nsu.mr.endpoints;

import com.google.gson.Gson;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import ru.nsu.mr.endpoints.dto.NewTaskDetails;
import ru.nsu.mr.endpoints.dto.TaskDetails;
import ru.nsu.mr.endpoints.dto.TaskInfo;

import java.io.*;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

public class WorkerEndpoint {
    private final Function<NewTaskDetails, TaskDetails> taskCreator;
    private final Function<Integer, TaskDetails> taskDetailsRetriever;
    private final Supplier<List<TaskInfo>> allTasksSupplier;
    private final Gson gson = new Gson();
    private HttpServer server;

    public WorkerEndpoint(
            Function<NewTaskDetails, TaskDetails> taskCreator,
            Function<Integer, TaskDetails> taskDetailsRetriever,
            Supplier<List<TaskInfo>> allTasksSupplier) {
        this.taskCreator = taskCreator;
        this.taskDetailsRetriever = taskDetailsRetriever;
        this.allTasksSupplier = allTasksSupplier;
    }

    public void startServer(int port) throws IOException {
        server = HttpServer.create(new InetSocketAddress(port), 0);
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

            if ("PUT".equalsIgnoreCase(requestMethod)) {
                handlePutRequest(exchange);
            } else if ("GET".equalsIgnoreCase(requestMethod)) {
                handleGetRequest(exchange);
            } else {
                sendResponse(exchange, 405, "Method Not Allowed");
            }
        }

        private void handlePutRequest(HttpExchange exchange) throws IOException {
            try (BufferedReader reader =
                    new BufferedReader(new InputStreamReader(exchange.getRequestBody()))) {
                NewTaskDetails taskRequest = gson.fromJson(reader, NewTaskDetails.class);
                TaskDetails taskDetails = taskCreator.apply(taskRequest);
                sendResponse(exchange, 200, gson.toJson(taskDetails));
            } catch (Exception e) {
                sendResponse(exchange, 400, "Failed to create task: " + e.getMessage());
            }
        }

        private void handleGetRequest(HttpExchange exchange) throws IOException {
            String path = exchange.getRequestURI().getPath();
            String[] segments = path.split("/");

            if (segments.length == 2) {
                handleGetAllTasks(exchange);
            } else if (segments.length == 3 && !segments[2].isEmpty()) {
                handleGetTaskById(exchange, segments[2]);
            } else {
                sendResponse(exchange, 400, "Invalid request path");
            }
        }

        private void handleGetAllTasks(HttpExchange exchange) throws IOException {
            try {
                List<TaskInfo> taskInfoList = allTasksSupplier.get();
                sendResponse(exchange, 200, gson.toJson(taskInfoList));
            } catch (Exception e) {
                sendResponse(exchange, 400, "Failed to retrieve tasks: " + e.getMessage());
            }
        }

        private void handleGetTaskById(HttpExchange exchange, String taskId) throws IOException {
            try {
                TaskDetails taskDetails = taskDetailsRetriever.apply(Integer.parseInt(taskId));
                if (taskDetails != null) {
                    sendResponse(exchange, 200, gson.toJson(taskDetails));
                } else {
                    sendResponse(exchange, 404, "Task not found");
                }
            } catch (Exception e) {
                sendResponse(exchange, 400, "Failed to retrieve task: " + e.getMessage());
            }
        }
    }

    private void sendResponse(HttpExchange exchange, int statusCode, String response)
            throws IOException {
        exchange.sendResponseHeaders(statusCode, response.getBytes().length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(response.getBytes());
        }
    }
}
