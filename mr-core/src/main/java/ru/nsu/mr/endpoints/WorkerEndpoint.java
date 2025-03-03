package ru.nsu.mr.endpoints;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import ru.nsu.mr.endpoints.dto.NewTaskDetails;
import ru.nsu.mr.endpoints.dto.TaskDetails;
import ru.nsu.mr.endpoints.dto.TaskStatusInfo;
import ru.nsu.mr.gateway.HttpUtils;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.List;

import static ru.nsu.mr.gateway.HttpUtils.sendJsonResponse;

public class WorkerEndpoint {
    public interface TaskService {
        TaskDetails createTask(NewTaskDetails details) throws Exception;
        TaskDetails getTaskDetails(int taskId);
        List<TaskStatusInfo> getAllTasks();
    }

    private static final int STATUS_OK = 200;
    private static final int STATUS_BAD_REQUEST = 400;
    private static final int STATUS_NOT_FOUND = 404;
    private static final int STATUS_METHOD_NOT_ALLOWED = 405;

    private final TaskService taskService;
    private final HttpServer server;

    public WorkerEndpoint(String workerBaseUrl, TaskService taskService) throws IOException {
        URI uri = URI.create(workerBaseUrl);
        server = HttpServer.create(new InetSocketAddress(uri.getHost(), uri.getPort()), 0);
        server.createContext("/tasks", new TasksHandler());
        this.taskService = taskService;
    }

    public void startServer() {
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
                    HttpUtils.sendErrorResponse(exchange, STATUS_METHOD_NOT_ALLOWED, "Method Not Allowed");
            }
        }

        private void handleCreateTask(HttpExchange exchange) throws IOException {
            try {
                NewTaskDetails newTaskDetails = HttpUtils.readRequestBody(exchange, NewTaskDetails.class);
                TaskDetails createdTask = taskService.createTask(newTaskDetails);
                sendJsonResponse(exchange, STATUS_OK, createdTask);
            } catch (Exception e) {
                HttpUtils.sendErrorResponse(exchange, STATUS_BAD_REQUEST, "Failed to create task: " + e.getMessage());
            }
        }

        private void handleRetrieveTask(HttpExchange exchange) throws IOException {
            String path = exchange.getRequestURI().getPath();
            String[] pathSegments = path.split("/");
            if (pathSegments.length == 2) {
                handleRetrieveAllTasks(exchange);
            } else if (pathSegments.length == 3 && !pathSegments[2].isEmpty()) {
                handleRetrieveTaskById(exchange, pathSegments[2]);
            } else {
                HttpUtils.sendErrorResponse(exchange, STATUS_BAD_REQUEST, "Invalid request path");
            }
        }

        private void handleRetrieveAllTasks(HttpExchange exchange) throws IOException {
            try {
                List<TaskStatusInfo> tasks = taskService.getAllTasks();
                sendJsonResponse(exchange, STATUS_OK, tasks);
            } catch (Exception e) {
                HttpUtils.sendErrorResponse(exchange, STATUS_BAD_REQUEST, "Failed to retrieve tasks: " + e.getMessage());
            }
        }

        private void handleRetrieveTaskById(HttpExchange exchange, String taskIdString) throws IOException {
            try {
                int taskId = Integer.parseInt(taskIdString);
                TaskDetails taskDetails = taskService.getTaskDetails(taskId);
                if (taskDetails != null) {
                    sendJsonResponse(exchange, STATUS_OK, taskDetails);
                } else {
                    HttpUtils.sendErrorResponse(exchange, STATUS_NOT_FOUND, "Task not found for ID: " + taskId);
                }
            } catch (NumberFormatException e) {
                HttpUtils.sendErrorResponse(exchange, STATUS_BAD_REQUEST, "Invalid task ID format: " + taskIdString);
            } catch (Exception e) {
                HttpUtils.sendErrorResponse(exchange, STATUS_BAD_REQUEST, "Failed to retrieve task: " + e.getMessage());
            }
        }
    }
}
