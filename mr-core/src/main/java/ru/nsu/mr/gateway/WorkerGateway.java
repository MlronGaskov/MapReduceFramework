package ru.nsu.mr.gateway;

import ru.nsu.mr.endpoints.dto.NewTaskDetails;
import ru.nsu.mr.endpoints.dto.TaskDetails;
import ru.nsu.mr.endpoints.dto.TaskStatusInfo;

import java.io.IOException;
import java.net.http.HttpClient;
import java.util.Arrays;
import java.util.List;

public class WorkerGateway {
    private final String workerBaseUrl;
    private final HttpClient httpClient;

    public WorkerGateway(String workerBaseUrl) {
        this.workerBaseUrl = workerBaseUrl;
        this.httpClient = HttpClient.newHttpClient();
    }

    public TaskDetails createTask(NewTaskDetails newTaskDetails) throws InterruptedException, IOException {
        return HttpUtils.sendPutRequest(
                httpClient,
                workerBaseUrl  + "/tasks",
                newTaskDetails,
                TaskDetails.class,
                "Failed to create task");
    }

    public TaskDetails getTaskDetails(int taskId) throws IOException, InterruptedException {
        return HttpUtils.sendGetRequest(
                httpClient,
                workerBaseUrl + "/tasks/" + taskId,
                TaskDetails.class,
                "Failed to get task details");
    }

    public List<TaskStatusInfo> getAllTasks() throws IOException, InterruptedException {
        TaskStatusInfo[] tasks = HttpUtils.sendGetRequest(
                httpClient,
                workerBaseUrl + "/tasks",
                TaskStatusInfo[].class,
                "Failed to get all tasks");
        return Arrays.asList(tasks);
    }

    public boolean isAlive() {
        try {
            String response = HttpUtils.sendGetRequest(
                    httpClient,
                    workerBaseUrl + "/health",
                    String.class,
                    "Failed to get health status");
            return "OK".equalsIgnoreCase(response);
        } catch (Exception e) {
            return false;
        }
    }

}
