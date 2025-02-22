package ru.nsu.mr;

import ru.nsu.mr.config.Configuration;
import ru.nsu.mr.config.ConfigurationOption;
import ru.nsu.mr.endpoints.WorkerEndpoint;
import ru.nsu.mr.endpoints.dto.*;
import ru.nsu.mr.gateway.CoordinatorGateway;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Stream;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.appender.ConsoleAppender;
import org.apache.logging.log4j.core.appender.FileAppender;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.builder.api.ConfigurationBuilderFactory;
import org.apache.logging.log4j.core.config.builder.api.ConfigurationBuilder;
import org.apache.logging.log4j.core.layout.PatternLayout;
import ru.nsu.mr.storages.StorageProvider;
import ru.nsu.mr.storages.StorageProviderFactory;

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
        private final List<String> inputFiles;
        private TaskStatus status;
        private final String storageConnectionString;
        private final String targetDir;

        public Task(
                int taskId,
                TaskType taskType,
                List<String> inputFiles,
                String storageConnectionString,
                String targetDir
        ) {
            this.taskId = taskId;
            this.taskType = taskType;
            this.inputFiles = inputFiles;
            this.status = TaskStatus.RUNNING;
            this.storageConnectionString = storageConnectionString;
            this.targetDir = targetDir.endsWith("/") ? targetDir : targetDir + "/";
        }

        public synchronized void setStatus(TaskStatus newStatus) {
            this.status = newStatus;
        }

        public synchronized TaskInfo getTaskInfo() {
            return new TaskInfo(
                    taskId,
                    taskType,
                    inputFiles,
                    status.toString());
        }

        public synchronized TaskDetails getTaskDetails() {
            return new TaskDetails(
                    taskId,
                    taskType,
                    inputFiles,
                    status.toString());
        }
    }

    private final MapReduceJob<?, ?, ?, ?> job;
    private final Configuration configuration;
    private final CoordinatorGateway coordinatorGateway;
    private final String serverPort;

    private volatile Task currentTask = null;
    private final Map<Integer, Task> previousTasks = new HashMap<>();
    private final WorkerEndpoint workerEndpoint;

    private static Logger LOGGER = null;
    private static LoggerContext context;
    private static final Object lock = new Object();
    private static boolean isConfigured = false;


    public Worker(
            MapReduceJob<?, ?, ?, ?> job,
            Configuration configuration,
            String serverPort,
            String coordinatorPort
    ) throws IOException {

        this.job = job;
        this.configuration = configuration;
        this.serverPort = serverPort;

        this.coordinatorGateway = new CoordinatorGateway(coordinatorPort);

        configureLogging();

        workerEndpoint =
                new WorkerEndpoint(this::createTask, this::getTaskDetails, this::getAllTasks);
        workerEndpoint.startServer(serverPort);
    }

    private void configureLogging() {
        synchronized (lock) {
            Path logPath = Path.of(configuration.get(ConfigurationOption.LOGS_PATH));

            if (logPath.toString().isEmpty()) {
                throw new IllegalArgumentException("Log path is not set in configuration.");
            }

            String logFileName = String.format("logs-worker-%s.log", this.serverPort);
            Path logFile = logPath.resolve(logFileName);

            if (!isConfigured) {
                ConfigurationBuilder<?> builder = ConfigurationBuilderFactory.newConfigurationBuilder();
                builder.setStatusLevel(Level.ERROR);
                builder.setConfigurationName("WorkerLogConfig");

                builder.add(builder.newRootLogger(Level.DEBUG));
                isConfigured = true;

                context = (LoggerContext) LogManager.getContext(false);
                context.start(builder.build());
            }

            String appenderName = "FileAppender-" + this.serverPort;
            Appender fileAppender = FileAppender.newBuilder()
                    .setName(appenderName)
                    .withFileName(logFile.toString())
                    .setLayout(PatternLayout.newBuilder()
                            .withPattern("%d [%t] %-5level: %msg%n%throwable")
                            .build())
                    .build();
            fileAppender.start();

            context.getConfiguration().addAppender(fileAppender);
            context.getConfiguration().getRootLogger().addAppender(fileAppender, Level.DEBUG, null);

            // Appender for console
            String consoleAppenderName = "ConsoleAppender";
            Appender consoleAppender = ConsoleAppender.newBuilder()
                    .setName(consoleAppenderName)
                    .setLayout(PatternLayout.newBuilder()
                            .withPattern("%d [%t] %-5level: %msg%n%throwable")
                            .build())
                    .build();
            consoleAppender.start();

            context.getConfiguration().addAppender(consoleAppender);
            context.getConfiguration().getRootLogger().addAppender(consoleAppender, Level.INFO, null);

            context.updateLoggers();
            LOGGER = LogManager.getLogger("worker-" + this.serverPort);
        }
    }

    private void registerWorkerWithCoordinator(String serverPort) throws IOException {
        try {
            LOGGER.debug("Registering worker on port: {}.", serverPort);
            coordinatorGateway.registerWorker(serverPort);
            LOGGER.info("Worker {} successfully registered with coordinator.", serverPort);
        } catch (Exception e) {
            LOGGER.error("Failed to register worker {} with coordinator: .", serverPort, e);
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
                        taskDetails.inputFiles(),
                        taskDetails.storageConnectionString(),
                        taskDetails.destinationDir());

        LOGGER.debug("On worker {} created task with id: {}, type: {}", serverPort,
                taskDetails.taskId(), taskDetails.taskType());

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
        Path tempDir = null;
        try {
            tempDir = Files.createTempDirectory("worker_" + serverPort + "_task_" + currentTask.taskId);
            if (currentTask.taskType.equals(TaskType.MAP)) {
                processMapTask(tempDir);
            } else if (currentTask.taskType.equals(TaskType.REDUCE)) {
                processReduceTask(tempDir);
            }
            LOGGER.info("Worker: {} finished executing task: {}", serverPort, currentTask.taskId);
            markTaskAsSucceeded();
        } catch (IOException e) {
            LOGGER.error("Task ID: {} failed with exception.", currentTask.taskId, e);
            markTaskAsFailed(e);
        } finally {
            if (tempDir != null) {
                try {
                    System.out.println(tempDir);
                    deleteDirectory(tempDir);
                } catch (IOException e) {
                    LOGGER.warn("Failed to delete temporary directory: {}", tempDir, e);
                }
            }
        }
    }

    private void processMapTask(Path tempDir) throws IOException {
        try (StorageProvider storageProvider = StorageProviderFactory.getStorageProvider(currentTask.storageConnectionString)) {
            LOGGER.info("Executing MAP task ID: {}.", currentTask.taskId);
            List<Path> localInputFiles = new ArrayList<>();
            for (String fileKey : currentTask.inputFiles) {
                Path localFile = tempDir.resolve(fileKey);
                storageProvider.get(fileKey, localFile);
                localInputFiles.add(localFile);
            }
            Path mapperOutputDir = tempDir.resolve("mapper_output");
            Files.createDirectories(mapperOutputDir);
            MapReduceTasksRunner.executeMapperTask(localInputFiles, currentTask.taskId, mapperOutputDir, configuration, job, LOGGER);
            try (Stream<Path> filesStream = Files.list(mapperOutputDir)) {
                for (Path localOutputFile : filesStream.toList()) {
                    String remoteFileName = localOutputFile.getFileName().toString();
                    storageProvider.put(localOutputFile, currentTask.targetDir + remoteFileName);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void processReduceTask(Path tempDir) throws IOException {
        try (StorageProvider storageProvider = StorageProviderFactory.getStorageProvider(currentTask.storageConnectionString)) {
            LOGGER.info("Executing REDUCE task ID: {}.", currentTask.taskId);
            List<Path> localInputFiles = new ArrayList<>();
            for (String fileKey : currentTask.inputFiles) {
                Path localFile = tempDir.resolve(fileKey);
                storageProvider.get(fileKey, localFile);
                localInputFiles.add(localFile);
            }
            Path reduceOutputDir = tempDir.resolve("reduce_output");
            Files.createDirectories(reduceOutputDir);
            int reduceTaskIndex = currentTask.taskId - configuration.get(ConfigurationOption.MAPPERS_COUNT);
            MapReduceTasksRunner.executeReduceTask(localInputFiles, reduceTaskIndex, reduceOutputDir, configuration, job, LOGGER);
            try (Stream<Path> filesStream = Files.list(reduceOutputDir)) {
                for (Path localOutputFile : filesStream.toList()) {
                    String remoteFileName = localOutputFile.getFileName().toString();
                    storageProvider.put(localOutputFile, currentTask.targetDir + remoteFileName);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
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
            coordinatorGateway.notifyTask(details);
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

    private static void deleteDirectory(Path path) throws IOException {
        if (Files.exists(path)) {
            Files.walk(path)
                    .sorted(Comparator.reverseOrder())
                    .forEach(p -> {
                        try {
                            Files.delete(p);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    });
        }
    }
}
