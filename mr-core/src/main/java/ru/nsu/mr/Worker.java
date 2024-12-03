package ru.nsu.mr;

import ru.nsu.mr.config.Configuration;
import ru.nsu.mr.config.ConfigurationOption;
import ru.nsu.mr.endpoints.WorkerEndpoint;
import ru.nsu.mr.endpoints.dto.*;
import ru.nsu.mr.manager.CoordinatorManager;

import java.io.IOException;
import java.nio.file.Path;
import java.util.*;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

public class Worker {

    private static final class Task {
        public enum TaskStatus {
            RUNNING,
            SUCCEED,
            FAILED;

            private Exception exception;

            public void setException(Exception e) {
                this.exception = e;
            }

            @Override
            public String toString() {
                return super.toString() + (exception != null ? ": " + exception : "");
            }
        }

        private final int taskId;
        private final TaskType taskType;
        private final List<Path> inputFiles;
        private TaskStatus status;

        public Task(int taskId, TaskType taskType, List<Path> inputFiles) {
            this.taskId = taskId;
            this.taskType = taskType;
            this.inputFiles = inputFiles;
            this.status = TaskStatus.RUNNING;
        }

        public synchronized void setStatus(TaskStatus newStatus) {
            this.status = newStatus;
        }

        public synchronized TaskInfo getTaskInfo() {
            return new TaskInfo(
                    taskId,
                    taskType,
                    inputFiles.stream().map(Path::toString).toList(),
                    status.toString());
        }

        public synchronized TaskDetails getTaskDetails() {
            return new TaskDetails(
                    taskId,
                    taskType,
                    inputFiles.stream().map(Path::toString).toList(),
                    status.toString());
        }
    }

    private final MapReduceJob<?, ?, ?, ?> job;
    private final Configuration configuration;
    private final Path outputDirectory;
    private final Path mappersOutputPath;
    private final CoordinatorManager coordinatorManager;
    private final String serverPort;

    private volatile Task currentTask = null;
    private final Map<Integer, Task> previousTasks = new HashMap<>();
    private final WorkerEndpoint workerEndpoint;

    private static final Logger LOGGER = LogManager.getLogger();

    public Worker(
            MapReduceJob<?, ?, ?, ?> job,
            Configuration configuration,
            Path mappersOutputDirectory,
            Path outputDirectory,
            String serverPort,
            String coordinatorPort)
            throws IOException {

        this.job = job;
        this.configuration = configuration;
        this.outputDirectory = outputDirectory;
        this.mappersOutputPath = mappersOutputDirectory;
        this.serverPort = serverPort;

        this.coordinatorManager = new CoordinatorManager(coordinatorPort);

        workerEndpoint =
                new WorkerEndpoint(this::createTask, this::getTaskDetails, this::getAllTasks);
        workerEndpoint.startServer(serverPort);
    }

    private void registerWorkerWithCoordinator(String serverPort) throws IOException {
        try {
            coordinatorManager.registerWorker(serverPort);
            LOGGER.info("Worker {} successfully registered with coordinator.", this.hashCode());
        } catch (Exception e) {
            LOGGER.error("Failed to register worker {} with coordinator: .", this.hashCode(), e);
            throw new IOException(
                    "Failed to register worker with coordinator: " + e.getMessage(), e);
        }
    }

    public synchronized TaskDetails createTask(NewTaskDetails taskDetails) {
        LOGGER.info("Creating task with ID: {}.", taskDetails.taskId());
        if (currentTask != null) {
            LOGGER.warn("The worker is currently busy with another task. Task ID: {}.",
                    taskDetails.taskId());
            throw new IllegalStateException("The worker is currently busy with another task.");
        }

        currentTask =
                new Task(
                        taskDetails.taskId(),
                        taskDetails.taskType(),
                        taskDetails.inputFiles().stream().map(Path::of).toList());

        notify();
        return currentTask.getTaskDetails();
    }

    public synchronized TaskDetails getTaskDetails(int taskId) {
        if (currentTask != null && currentTask.taskId == taskId) {
            return currentTask.getTaskDetails();
        }
        return previousTasks.getOrDefault(taskId, null).getTaskDetails();
    }

    public synchronized List<TaskInfo> getAllTasks() {
        List<TaskInfo> result =
                new ArrayList<>(previousTasks.values().stream().map(Task::getTaskInfo).toList());
        if (currentTask != null) {
            result.add(currentTask.getTaskInfo());
        }
        return result;
    }

    public void start() throws IOException {
        registerWorkerWithCoordinator(serverPort);
        while (!Thread.currentThread().isInterrupted()) {
            try {
                waitForTask();
                executeCurrentTask();
                TaskDetails details = currentTask.getTaskDetails();
                moveCurrentTaskToHistory();
                notifyTaskEndToCoordinator(details);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        workerEndpoint.stopServer();
    }

    private synchronized void waitForTask() throws InterruptedException {
        while (currentTask == null) {
            wait();
        }
    }

    private void executeCurrentTask() {
        try {
            if (currentTask.taskType.equals(TaskType.MAP)) {
                LOGGER.info("Executing MAP task ID: {}.", currentTask.taskId);
                MapReduceTasksRunner.executeMapperTask(
                        currentTask.inputFiles,
                        currentTask.taskId,
                        mappersOutputPath,
                        configuration,
                        job);
            } else if (currentTask.taskType.equals(TaskType.REDUCE)) {
                LOGGER.info("Executing REDUCE task ID: {}.", currentTask.taskId);
                MapReduceTasksRunner.executeReduceTask(
                        currentTask.inputFiles.stream().map(mappersOutputPath::resolve).toList(),
                        currentTask.taskId - configuration.get(ConfigurationOption.MAPPERS_COUNT),
                        outputDirectory,
                        configuration,
                        job);
            }
            markTaskAsSucceeded();
        } catch (IOException e) {
            LOGGER.error("Task ID: {} failed with exception.", currentTask.taskId, e);
            markTaskAsFailed(e);
        }
    }

    private synchronized void markTaskAsSucceeded() {
        currentTask.setStatus(Task.TaskStatus.SUCCEED);
    }

    private synchronized void markTaskAsFailed(Exception e) {
        currentTask.setStatus(Task.TaskStatus.FAILED);
        currentTask.status.setException(e);
    }

    private void notifyTaskEndToCoordinator(TaskDetails details) {
        try {
            coordinatorManager.notifyTask(details);
            LOGGER.info("Notified coordinator about task completion for Task ID: {}.",
                    details.taskId());
        } catch (IOException | InterruptedException e) {
            LOGGER.error("Failed to notify task completion for Task ID: {}.",details.taskId(), e);
        }
    }

    private synchronized void moveCurrentTaskToHistory() {
        LOGGER.info("Moving task ID: {} to history.", currentTask.taskId);
        previousTasks.put(currentTask.taskId, currentTask);
        currentTask = null;
    }
}
