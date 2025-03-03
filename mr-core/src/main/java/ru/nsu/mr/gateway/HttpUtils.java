package ru.nsu.mr.gateway;

import com.google.gson.Gson;
import com.sun.net.httpserver.HttpExchange;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;

public class HttpUtils {

    private HttpUtils() {}

    private static final int STATUS_OK = 200;

    private static final Gson gson = new Gson();

    public static <T> T readRequestBody(HttpExchange exchange, Class<T> clazz) throws IOException {
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(exchange.getRequestBody(), StandardCharsets.UTF_8))) {
            return gson.fromJson(reader, clazz);
        }
    }

    public static void sendJsonResponse(HttpExchange exchange, int statusCode, Object response) throws IOException {
        String jsonResponse = gson.toJson(response);
        exchange.getResponseHeaders().set("Content-Type", "application/json; charset=UTF-8");
        byte[] responseBytes = jsonResponse.getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(statusCode, responseBytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(responseBytes);
        }
    }

    public static void sendResponse(HttpExchange exchange, int statusCode, String responseMessage) throws IOException {
        byte[] responseBytes = responseMessage.getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(statusCode, responseBytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(responseBytes);
        }
    }

    public static void sendErrorResponse(HttpExchange exchange, int statusCode, String errorMessage) throws IOException {
        exchange.getResponseHeaders().set("Content-Type", "text/plain; charset=UTF-8");
        byte[] errorBytes = errorMessage.getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(statusCode, errorBytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(errorBytes);
        }
    }

    public static <T> T sendGetRequest(HttpClient httpClient, String endpoint, Class<T> responseType, String errorMsg)
            throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder().uri(URI.create(endpoint)).GET().build();
        HttpResponse<String> response =
                httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() == STATUS_OK) {
            return gson.fromJson(response.body(), responseType);
        } else {
            throw new IOException(errorMsg + ": " + response.body());
        }
    }

    public static <T> T sendPutRequest(
            HttpClient httpClient,
            String endpoint,
            Object payload,
            Class<T> responseType,
            String errorMsg)
            throws IOException, InterruptedException {
        String requestBody = gson.toJson(payload);

        HttpRequest request =
                HttpRequest.newBuilder()
                        .uri(URI.create(endpoint))
                        .header("Content-Type", "application/json")
                        .PUT(HttpRequest.BodyPublishers.ofString(requestBody))
                        .build();

        HttpResponse<String> response =
                httpClient.send(request, HttpResponse.BodyHandlers.ofString());

        if (response.statusCode() == STATUS_OK) {
            return gson.fromJson(response.body(), responseType);
        } else {
            throw new IOException(errorMsg + ": " + response.body());
        }
    }

    public static <T> T sendPostRequest(
            HttpClient httpClient,
            String endpoint,
            Object payload,
            Class<T> responseType,
            String errorMsg)
            throws IOException, InterruptedException {
        String requestBody = gson.toJson(payload);

        HttpRequest request =
                HttpRequest.newBuilder()
                        .uri(URI.create(endpoint))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(requestBody))
                        .build();

        HttpResponse<String> response =
                httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() == STATUS_OK) {
            if (responseType == Void.class) {
                return null;
            }
            return gson.fromJson(response.body(), responseType);
        } else {
            throw new IOException(errorMsg + ": " + response.body());
        }
    }
}
