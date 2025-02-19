package ru.nsu.mr;

import static org.junit.jupiter.api.Assertions.*;

import com.google.gson.Gson;

import org.junit.jupiter.api.*;

import ru.nsu.mr.endpoints.dto.NewTaskDetails;
import ru.nsu.mr.endpoints.dto.TaskDetails;
import ru.nsu.mr.endpoints.dto.TaskInfo;
import ru.nsu.mr.endpoints.dto.TaskType;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Supplier;

class WorkerEndpointTest {

    private static final Gson gson = new Gson();
    private WorkerEndpoint workerEndpoint;

    private final ConcurrentHashMap<Integer, TaskDetails> taskStorage = new ConcurrentHashMap<>();
    private final List<TaskInfo> allTasksList = new ArrayList<>();

    @BeforeEach
    void setUp() throws IOException {
        Function<NewTaskDetails, TaskDetails> taskCreator =
                (NewTaskDetails newTask) -> {
                    TaskDetails taskDetails =
                            new TaskDetails(
                                    newTask.taskId(),
                                    newTask.taskType(),
                                    newTask.inputFiles(),
                                    "CREATED");
                    taskStorage.put(newTask.taskId(), taskDetails);
                    allTasksList.add(
                            new TaskInfo(
                                    newTask.taskId(),
                                    newTask.taskType(),
                                    newTask.inputFiles(),
                                    "CREATED"));
                    return taskDetails;
                };

        Function<Integer, TaskDetails> taskDetailsRetriever = taskStorage::get;

        Supplier<List<TaskInfo>> allTasksSupplier = () -> new ArrayList<>(allTasksList);

        workerEndpoint = new WorkerEndpoint(taskCreator, taskDetailsRetriever, allTasksSupplier);
        workerEndpoint.startServer("8080");
    }

    @AfterEach
    void tearDown() {
        taskStorage.clear();
        allTasksList.clear();
        workerEndpoint.stopServer();
    }

    @Test
    void testPutRequestCreatesTask() throws IOException {
        NewTaskDetails newTask = new NewTaskDetails(1, TaskType.MAP, List.of("input1.txt"), null);
        HttpURLConnection connection =
                (HttpURLConnection) new URL("http://localhost:8080/tasks").openConnection();
        connection.setRequestMethod("PUT");
        connection.setDoOutput(true);
        connection.setRequestProperty("Content-Type", "application/json");

        try (OutputStream os = connection.getOutputStream()) {
            os.write(gson.toJson(newTask).getBytes());
        }

        int responseCode = connection.getResponseCode();
        assertEquals(200, responseCode);

        try (InputStream is = connection.getInputStream()) {
            TaskDetails response = gson.fromJson(new InputStreamReader(is), TaskDetails.class);
            assertEquals(newTask.taskId(), response.taskId());
            assertEquals(newTask.taskType(), response.taskType());
            assertEquals(newTask.inputFiles(), response.inputFiles());
            assertEquals("CREATED", response.status());
        }

        TaskDetails storedTask = taskStorage.get(newTask.taskId());
        assertNotNull(storedTask);
        assertEquals("CREATED", storedTask.status());
    }

    @Test
    void testGetRequestReturnsAllTasks() throws IOException {
        taskStorage.put(1, new TaskDetails(1, TaskType.MAP, List.of("file1.txt"), "CREATED"));
        taskStorage.put(
                2, new TaskDetails(2, TaskType.REDUCE, List.of("file2.txt"), "IN_PROGRESS"));
        allTasksList.add(new TaskInfo(1, TaskType.MAP, List.of("file1.txt"), "CREATED"));
        allTasksList.add(new TaskInfo(2, TaskType.REDUCE, List.of("file2.txt"), "IN_PROGRESS"));

        HttpURLConnection connection =
                (HttpURLConnection) new URL("http://localhost:8080/tasks").openConnection();
        connection.setRequestMethod("GET");

        int responseCode = connection.getResponseCode();
        assertEquals(200, responseCode);

        try (InputStream is = connection.getInputStream()) {
            List<TaskInfo> response =
                    List.of(gson.fromJson(new InputStreamReader(is), TaskInfo[].class));
            assertEquals(2, response.size());
            assertEquals(1, response.get(0).taskId());
            assertEquals(2, response.get(1).taskId());
        }
    }

    @Test
    void testGetRequestByIdReturnsTask() throws IOException {
        taskStorage.put(1, new TaskDetails(1, TaskType.MAP, List.of("file1.txt"), "CREATED"));

        HttpURLConnection connection =
                (HttpURLConnection) new URL("http://localhost:8080/tasks/1").openConnection();
        connection.setRequestMethod("GET");

        int responseCode = connection.getResponseCode();
        assertEquals(200, responseCode);

        try (InputStream is = connection.getInputStream()) {
            TaskDetails response = gson.fromJson(new InputStreamReader(is), TaskDetails.class);
            assertEquals(1, response.taskId());
            assertEquals(TaskType.MAP, response.taskType());
            assertEquals(List.of("file1.txt"), response.inputFiles());
            assertEquals("CREATED", response.status());
        }
    }

    @Test
    void testGetRequestByIdTaskNotFound() throws IOException {
        HttpURLConnection connection =
                (HttpURLConnection) new URL("http://localhost:8080/tasks/999").openConnection();
        connection.setRequestMethod("GET");

        int responseCode = connection.getResponseCode();
        assertEquals(404, responseCode);

        try (InputStream is = connection.getErrorStream()) {
            assertNotNull(is);
        }
    }

    @Test
    void testUnsupportedMethodReturnsMethodNotAllowed() throws IOException {
        HttpURLConnection connection =
                (HttpURLConnection) new URL("http://localhost:8080/tasks").openConnection();
        connection.setRequestMethod("POST");

        int responseCode = connection.getResponseCode();
        assertEquals(405, responseCode);

        try (InputStream is = connection.getErrorStream()) {
            assertNotNull(is);
        }
    }
}
