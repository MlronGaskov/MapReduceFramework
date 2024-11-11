package ru.nsu.mr.endpoints;

import com.google.gson.Gson;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import ru.nsu.mr.endpoints.dto.Task;

import java.io.*;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

public class WorkerEndpoint {
    private final WorkerManager workerManager;
    private final Gson gson = new Gson();

    public WorkerEndpoint(WorkerManager workerManager) {
        this.workerManager = workerManager;
    }

    public void startServer(int port) throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/task/issue", new TaskIssueHandler());
        server.createContext("/status", new StatusHandler());
        server.setExecutor(null);
        server.start();
    }

    private class TaskIssueHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if ("PUT".equals(exchange.getRequestMethod())) {
                BufferedReader reader =
                        new BufferedReader(new InputStreamReader(exchange.getRequestBody()));
                Task taskRequest = gson.fromJson(reader, Task.class);

                int taskId = taskRequest.getTaskId();
                WorkerManager.TaskType taskType = taskRequest.getTaskType();
                List<Path> inputFiles = taskRequest.getInputFiles();

                Optional<String> error = workerManager.addTask(taskId, taskType, inputFiles);

                String response = error.orElse("Task issued successfully");
                int statusCode = error.isPresent() ? 409 : 200;
                exchange.sendResponseHeaders(statusCode, response.getBytes().length);
                exchange.getResponseBody().write(response.getBytes());
                exchange.getResponseBody().close();
            } else {
                exchange.sendResponseHeaders(405, -1);
            }
        }
    }

    private class StatusHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            String response =
                    workerManager.getCurrentTask().isPresent()
                            ? "Task in progress"
                            : "No task in progress";
            exchange.sendResponseHeaders(200, response.getBytes().length);
            OutputStream os = exchange.getResponseBody();
            os.write(response.getBytes());
            os.close();
        }
    }
}
