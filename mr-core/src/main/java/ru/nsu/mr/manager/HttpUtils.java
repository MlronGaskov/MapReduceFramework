package ru.nsu.mr.manager;

import com.google.gson.Gson;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

public class HttpUtils {
    private static final int STATUS_OK = 200;

    private HttpUtils() {}

    public static String sendGetRequest(HttpClient httpClient, String endpoint, String errorMsg)
            throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder().uri(URI.create(endpoint)).GET().build();

        HttpResponse<String> response =
                httpClient.send(request, HttpResponse.BodyHandlers.ofString());

        if (response.statusCode() == STATUS_OK) {
            return response.body();
        } else {
            throw new IOException(errorMsg + ": " + response.body());
        }
    }

    public static <T> T sendPutRequest(
            HttpClient httpClient,
            Gson gson,
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
            Gson gson,
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
