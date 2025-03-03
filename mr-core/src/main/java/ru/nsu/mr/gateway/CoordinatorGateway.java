package ru.nsu.mr.gateway;

import ru.nsu.mr.endpoints.dto.TaskDetails;

import java.io.IOException;
import java.net.http.HttpClient;

public class CoordinatorGateway {
    private final String coordinatorBaseUrl;
    private final HttpClient httpClient;

    public CoordinatorGateway(String coordinatorBaseUrl) {
        this.coordinatorBaseUrl = coordinatorBaseUrl;
        this.httpClient = HttpClient.newHttpClient();
    }

    public void registerWorker(String workerBaseUrl) throws IOException, InterruptedException {
        String endpoint = coordinatorBaseUrl + "/workers";
        HttpUtils.sendPostRequest(
                httpClient, endpoint, workerBaseUrl, Void.class, "Failed to register worker");
    }

    public void notifyTask(TaskDetails taskDetails) throws IOException, InterruptedException {
        String endpoint = coordinatorBaseUrl + "/notifyTask";
        HttpUtils.sendPostRequest(
                httpClient, endpoint, taskDetails, Void.class, "Failed to notify task completion");
    }
}
