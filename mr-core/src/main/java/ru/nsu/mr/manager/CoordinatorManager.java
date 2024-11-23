package ru.nsu.mr.manager;

import com.google.gson.Gson;

import ru.nsu.mr.endpoints.dto.TaskDetails;

import java.io.IOException;
import java.net.http.HttpClient;

public class CoordinatorManager {
    private final String coordinatorBaseUrl;
    private final HttpClient httpClient;
    private final Gson gson;

    public CoordinatorManager(String port) {
        this.coordinatorBaseUrl = "http://localhost:" + port;
        this.httpClient = HttpClient.newHttpClient();
        this.gson = new Gson();
    }

    public void registerWorker(String workerPort) throws IOException, InterruptedException {
        String endpoint = coordinatorBaseUrl + "/registerWorker";
        HttpUtils.sendPostRequest(
                httpClient, gson, endpoint, workerPort, Void.class, "Failed to register worker");
    }

    public void notifyTask(TaskDetails taskDetails) throws IOException, InterruptedException {
        String endpoint = coordinatorBaseUrl + "/notifyTask";
        HttpUtils.sendPostRequest(
                httpClient,
                gson,
                endpoint,
                taskDetails,
                Void.class,
                "Failed to notify task completion");
    }

    @Override
    public String toString() {
        return "CoordinatorManager{" + "coordinatorBaseUrl='" + coordinatorBaseUrl + '\'' + '}';
    }
}
