package ru.nsu.mr.endpoints;

import com.google.gson.Gson;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import ru.nsu.mr.Worker.Task;

import java.io.*;
import java.net.InetSocketAddress;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class WorkerEndpoint {
    private final Consumer<Task> taskConsumer;
    private final Supplier<Boolean> isTaskInProgress;
    private final Gson gson = new Gson();

    public WorkerEndpoint(Consumer<Task> taskConsumer, Supplier<Boolean> isTaskInProgress) {
        this.taskConsumer = taskConsumer;
        this.isTaskInProgress = isTaskInProgress;
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

                taskConsumer.accept(taskRequest);

                String response = "Task issued successfully";
                exchange.sendResponseHeaders(200, response.getBytes().length);
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
            String response = isTaskInProgress.get() ? "Task in progress" : "No task in progress";
            exchange.sendResponseHeaders(200, response.getBytes().length);
            OutputStream os = exchange.getResponseBody();
            os.write(response.getBytes());
            os.close();
        }
    }
}
