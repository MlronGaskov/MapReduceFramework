package ru.nsu.mr;

import ru.nsu.mr.config.Configuration;
import ru.nsu.mr.endpoints.*;
import ru.nsu.mr.endpoints.dto.*;

import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

public class Worker<KEY_INTER, VALUE_INTER, KEY_OUT, VALUE_OUT> {

    private record Task(int taskId, TaskType taskType, List<Path> inputFiles) {}

    private final MapReduceJob<KEY_INTER, VALUE_INTER, KEY_OUT, VALUE_OUT> job;
    private final Configuration configuration;
    private final Path outputDirectory;
    private final Path mappersOutputPath;
    private final BlockingQueue<Task> taskQueue = new LinkedBlockingQueue<>();

    public Worker(
            MapReduceJob<KEY_INTER, VALUE_INTER, KEY_OUT, VALUE_OUT> job,
            Configuration configuration,
            Path mappersOutputDirectory,
            Path outputDirectory,
            int serverPort)
            throws IOException {
        this.job = job;
        this.configuration = configuration;
        this.outputDirectory = outputDirectory;
        this.mappersOutputPath = mappersOutputDirectory;

        WorkerEndpoint workerEndpoint =
                new WorkerEndpoint(this::createTask, this::getTaskDetails, this::getAllTasks);
        workerEndpoint.startServer(serverPort);
    }

    private final Map<Integer, Task> tasks = new HashMap<>();

    public TaskDetails createTask(NewTaskDetails taskDetails) {
        Task task =
                new Task(
                        taskDetails.getTaskId(),
                        taskDetails.getTaskType(),
                        taskDetails.getInputFiles());
        tasks.put(task.taskId(), task);
        taskQueue.offer(task);
        return new TaskDetails(task.taskId(), task.taskType(), task.inputFiles);
    }

    public TaskDetails getTaskDetails(String taskId) {
        Task task = tasks.get(Integer.parseInt(taskId));
        if (task != null) {
            return new TaskDetails(task.taskId(), task.taskType(), task.inputFiles);
        }
        return null;
    }

    public List<TaskInfo> getAllTasks() {
        return tasks.values().stream()
                .map(task -> new TaskInfo(task.taskId(), task.taskType(), task.inputFiles))
                .collect(Collectors.toList());
    }

    public void start() {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                Task task = taskQueue.take();
                executeTask(task);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void executeTask(Task task) {
        try {
            if (task.taskType().equals(TaskType.MAP)) {
                MapReduceTasksRunner.executeMapperTask(
                        task.inputFiles(), task.taskId(), mappersOutputPath, configuration, job);
            } else if (task.taskType().equals(TaskType.REDUCE)) {
                MapReduceTasksRunner.executeReduceTask(
                        task.inputFiles(), task.taskId(), outputDirectory, configuration, job);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
