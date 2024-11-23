package ru.nsu.mr.manager;

import com.google.gson.Gson;

import ru.nsu.mr.endpoints.dto.NewTaskDetails;
import ru.nsu.mr.endpoints.dto.TaskDetails;

import java.io.IOException;
import java.net.http.HttpClient;

public class WorkerManager {
    private final String workerBaseUrl;
    private final HttpClient httpClient;
    private final Gson gson;

    public WorkerManager(String port) {
        this.workerBaseUrl = "http://localhost:" + port + "/tasks";
        this.httpClient = HttpClient.newHttpClient();
        this.gson = new Gson();
    }

    public TaskDetails createTask(NewTaskDetails newTaskDetails)
            throws InterruptedException, IOException {
        return HttpUtils.sendPutRequest(
                httpClient,
                gson,
                workerBaseUrl,
                newTaskDetails,
                TaskDetails.class,
                "Failed to create task");
    }
}
