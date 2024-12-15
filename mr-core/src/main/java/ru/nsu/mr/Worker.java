package ru.nsu.mr;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.SimpleLayout;

import ru.nsu.mr.config.Configuration;
import ru.nsu.mr.config.ConfigurationOption;
import ru.nsu.mr.endpoints.WorkerEndpoint;
import ru.nsu.mr.endpoints.dto.*;
import ru.nsu.mr.gateway.CoordinatorGateway;

import java.io.IOException;
import java.nio.file.Path;
import java.util.*;

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
    private final CoordinatorGateway coordinatorGateway;
    private final String serverPort;

    private volatile Task currentTask = null;
    private final Map<Integer, Task> previousTasks = new HashMap<>();
    private final WorkerEndpoint workerEndpoint;

    private static final Object lock = new Object();

    // Создаем логгер
    private static final Logger LOG = Logger.getLogger(Worker.class);

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

        initLogging(); // Инициализация логгирования

        LOG.info("Initializing worker on port: " + serverPort);

        this.coordinatorGateway = new CoordinatorGateway(coordinatorPort);

        workerEndpoint =
                new WorkerEndpoint(this::createTask, this::getTaskDetails, this::getAllTasks);
        workerEndpoint.startServer(serverPort);
    }

    private void initLogging() throws IOException {
        String logsPath = configuration.get(ConfigurationOption.LOGS_PATH);
        String logFileName = logsPath + "/worker" + serverPort + ".log";

        SimpleLayout layout = new SimpleLayout();

        FileAppender fileAppender = new FileAppender(layout, logFileName, true);
        ConsoleAppender consoleAppender = new ConsoleAppender(layout);

        Logger rootLogger = Logger.getRootLogger();
        rootLogger.setLevel(Level.DEBUG);
        rootLogger.addAppender(fileAppender);
        rootLogger.addAppender(consoleAppender);
    }

    private void registerWorkerWithCoordinator(String serverPort) throws IOException {
        LOG.info("Registering worker with coordinator on port: " + serverPort);
        try {
            coordinatorGateway.registerWorker(serverPort);
        } catch (Exception e) {
            LOG.error("Failed to register worker with coordinator: " + e.getMessage(), e);
            throw new IOException(
                    "Failed to register worker with coordinator: " + e.getMessage(), e);
        }
    }

    public synchronized TaskDetails createTask(NewTaskDetails taskDetails) {
        LOG.info("Received request to create task " + taskDetails.taskId()
                + " of type " + taskDetails.taskType());

        if (currentTask != null) {
            LOG.warn("Worker is busy, cannot accept new task " + taskDetails.taskId());
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
        LOG.debug("Request for details of task: " + taskId);
        if (currentTask != null && currentTask.taskId == taskId) {
            return currentTask.getTaskDetails();
        }
        Task previous = previousTasks.get(taskId);
        return previous != null ? previous.getTaskDetails() : null;
    }

    public synchronized List<TaskInfo> getAllTasks() {
        LOG.debug("Request for all tasks info");
        List<TaskInfo> result =
                new ArrayList<>(previousTasks.values().stream().map(Task::getTaskInfo).toList());
        if (currentTask != null) {
            result.add(currentTask.getTaskInfo());
        }
        return result;
    }

    public void start() throws IOException {
        LOG.info("Starting worker main loop on port " + serverPort);
        registerWorkerWithCoordinator(serverPort);
        while (!Thread.currentThread().isInterrupted()) {
            try {
                waitForTask();
                LOG.info("New task started: " + currentTask.taskId + " type: " + currentTask.taskType);
                executeCurrentTask();
                TaskDetails details = currentTask.getTaskDetails();
                moveCurrentTaskToHistory();
                notifyTaskEndToCoordinator(details);
                LOG.info("Task finished: " + details.taskId() + " with status " + details.status());
            } catch (InterruptedException e) {
                LOG.info("Worker interrupted, shutting down.");
                Thread.currentThread().interrupt();
            }
        }
        workerEndpoint.stopServer();
        LOG.info("Worker endpoint stopped.");
    }

    private synchronized void waitForTask() throws InterruptedException {
        LOG.debug("Waiting for a new task...");
        while (currentTask == null) {
            wait();
        }
    }

    private void executeCurrentTask() {
        try {
            if (currentTask.taskType.equals(TaskType.MAP)) {
                LOG.info("Executing MAP task " + currentTask.taskId);
                MapReduceTasksRunner.executeMapperTask(
                        currentTask.inputFiles,
                        currentTask.taskId,
                        mappersOutputPath,
                        configuration,
                        job);
            } else if (currentTask.taskType.equals(TaskType.REDUCE)) {
                LOG.info("Executing REDUCE task " + currentTask.taskId);
                MapReduceTasksRunner.executeReduceTask(
                        currentTask.inputFiles.stream().map(mappersOutputPath::resolve).toList(),
                        currentTask.taskId - configuration.get(ConfigurationOption.MAPPERS_COUNT),
                        outputDirectory,
                        configuration,
                        job);
            }
            markTaskAsSucceeded();
        } catch (IOException e) {
            LOG.error("Task " + currentTask.taskId + " failed: " + e.getMessage(), e);
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
            LOG.info("Notifying coordinator about the completion of task " + details.taskId());
            coordinatorGateway.notifyTask(details);
        } catch (IOException | InterruptedException e) {
            LOG.error("Failed to notify coordinator about task " + details.taskId() + ": " + e.getMessage(), e);
        }
    }

    private synchronized void moveCurrentTaskToHistory() {
        previousTasks.put(currentTask.taskId, currentTask);
        currentTask = null;
    }
}
