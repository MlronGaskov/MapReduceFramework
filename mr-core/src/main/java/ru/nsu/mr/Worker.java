package ru.nsu.mr;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.ConsoleAppender;
import org.apache.logging.log4j.core.appender.FileAppender;
import org.apache.logging.log4j.core.config.builder.api.ConfigurationBuilder;
import org.apache.logging.log4j.core.config.builder.api.ConfigurationBuilderFactory;
import org.apache.logging.log4j.core.appender.HttpAppender;
import org.apache.logging.log4j.core.layout.JsonLayout;
import org.apache.logging.log4j.core.layout.PatternLayout;
import ru.nsu.mr.config.Configuration;
import ru.nsu.mr.config.ConfigurationOption;
import ru.nsu.mr.endpoints.dto.*;
import ru.nsu.mr.endpoints.WorkerEndpoint;
import ru.nsu.mr.gateway.CoordinatorGateway;
import ru.nsu.mr.storages.StorageProvider;
import ru.nsu.mr.storages.StorageProviderFactory;
import ru.nsu.mr.endpoints.WorkerEndpoint.TaskService;

import java.io.IOException;
import java.net.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

public class Worker {
    private static final class Task {
        private enum TaskStatus {
            RUNNING,
            SUCCEED,
            FAILED;

            private Exception exception;

            private void setException(Exception e) {
                this.exception = e;
            }

            @Override
            public String toString() {
                return super.toString() + (exception != null ? ": " + exception : "");
            }
        }

        private final int jobId;
        private final MapReduceJob<?, ?, ?, ?> job;
        private final Configuration jobConfiguration;
        private final TaskInformation taskInformation;
        private TaskStatus status;

        private Task(int jobId,
                     MapReduceJob<?, ?, ?, ?> job,
                     Configuration jobConfiguration,
                     TaskInformation taskInformation) {
            this.jobId = jobId;
            this.job = job;
            this.jobConfiguration = jobConfiguration;
            this.taskInformation = taskInformation;
            this.status = TaskStatus.RUNNING;
        }

        private synchronized void setStatus(TaskStatus newStatus) {
            this.status = newStatus;
        }

        private synchronized TaskStatusInfo toTaskStatusInfo() {
            int taskId = taskInformation.taskId();
            TaskType type = taskInformation.taskType();
            return new TaskStatusInfo(jobId, taskId, type, status.toString());
        }

        private synchronized TaskDetails toTaskDetails() {
            return new TaskDetails(jobId, taskInformation, status.toString());
        }
    }

    private volatile MapReduceJob<?, ?, ?, ?> pendingJob = null;
    private volatile Configuration pendingJobConfig = null;
    private final CoordinatorGateway coordinatorV2Gateway;
    private final WorkerEndpoint workerEndpoint;
    private final String workerBaseUrl;
    private volatile Task currentTask = null;
    private final Map<Integer, Task> previousTasks = new HashMap<>();
    private static Logger LOGGER = null;
    private static LoggerContext loggerContext;
    private static final Object logLock = new Object();
    private static boolean loggingConfigured = false;

    public Worker(String coordinatorBaseUrl, String workerBaseUrl, String logsPath) throws IOException {
        this.workerBaseUrl = workerBaseUrl;
        this.coordinatorV2Gateway = !Objects.equals(coordinatorBaseUrl, "") ?
                new CoordinatorGateway(coordinatorBaseUrl) : null;
        TaskService taskService = new InMemoryTaskService();
        this.workerEndpoint = new WorkerEndpoint(workerBaseUrl, taskService);
        workerEndpoint.startServer();
        try {
            configureLogging(logsPath);
        } catch (URISyntaxException ignored) {
        }
    }

    synchronized void setJob(MapReduceJob<?, ?, ?, ?> job, Configuration jobConfiguration) {
        this.pendingJob = job;
        this.pendingJobConfig = jobConfiguration;
    }

    public void start() {
        registerWorkerWithCoordinator();
        while (!Thread.currentThread().isInterrupted()) {
            try {
                waitForTask();
                executeCurrentTask();
                TaskDetails details = currentTask.toTaskDetails();
                moveCurrentTaskToHistory();
                notifyTaskCompletion(details);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        workerEndpoint.stopServer();
    }

    synchronized public TaskDetails createTaskInternal(NewTaskDetails details) {
        LOGGER.info("Creating task with ID: {}.", details.taskInformation().taskId());
        if (currentTask != null) {
            LOGGER.warn("Worker is busy. Cannot create task with ID: {}.", details.taskInformation().taskId());
            throw new IllegalStateException("The worker is currently busy with another task.");
        }
        if (pendingJob == null || pendingJobConfig == null) {
            JobInformation jobInfo = details.jobInformation();
            try (StorageProvider jobStorage = StorageProviderFactory.getStorageProvider(jobInfo.storageConnectionString())) {
                try (TemporaryDirectory tempDir = new TemporaryDirectory(jobStorage)) {
                    Path jarFile = tempDir.get(List.of(jobInfo.userJobPath()), "jar").getFirst();
                    JarFileParser jarParser = new JarFileParser(jarFile);
                    pendingJob = jarParser.loadUsersSubClass(MapReduceJob.class);
                    jarParser.close();
                }
            } catch (Exception e) {
                throw new RuntimeException("Failed to load job from jar", e);
            }
            Configuration newConfig = new Configuration();
            newConfig.set(ConfigurationOption.MAPPERS_COUNT, jobInfo.mappersCount());
            newConfig.set(ConfigurationOption.REDUCERS_COUNT, jobInfo.reducersCount());
            newConfig.set(ConfigurationOption.SORTER_IN_MEMORY_RECORDS, jobInfo.sorterInMemoryRecords());
            pendingJobConfig = newConfig;
        }
        currentTask = new Task(details.jobInformation().jobId(), pendingJob, pendingJobConfig, details.taskInformation());
        LOGGER.debug("Worker {} created task with id: {} and type: {}.", workerBaseUrl,
                details.taskInformation().taskId(), details.taskInformation().taskType());
        notify();
        return currentTask.toTaskDetails();
    }

    synchronized public TaskDetails getTaskDetailsInternal(int taskId) {
        if (currentTask != null && currentTask.taskInformation.taskId() == taskId) {
            return currentTask.toTaskDetails();
        }
        Task task = previousTasks.get(taskId);
        return task != null ? task.toTaskDetails() : null;
    }

    synchronized public List<TaskStatusInfo> getAllTasksInternal() {
        List<TaskStatusInfo> result = previousTasks.values().stream()
                .map(Task::toTaskStatusInfo)
                .collect(Collectors.toList());
        if (currentTask != null) {
            result.add(currentTask.toTaskStatusInfo());
        }
        return result;
    }

    private void configureLogging(String logDestination) throws IOException, URISyntaxException {
        synchronized (logLock) {
            boolean logToEs = logDestination.startsWith("http://") || logDestination.startsWith("https://");
            String sanitizedWorkerId = workerBaseUrl
                    .replaceAll("https?://", "")
                    .replaceAll("[^a-zA-Z0-9.-]", "_");

            if (logToEs) {
                try {
                    URI uri = new URI(logDestination);
                    URI healthUri = new URI(uri.getScheme(), uri.getAuthority(), "/", null, null);
                    HttpURLConnection conn = (HttpURLConnection) healthUri.toURL().openConnection();
                    conn.setRequestMethod("HEAD");
                    conn.setConnectTimeout(2000);
                    conn.setReadTimeout(2000);
                    conn.connect();
                    int code = conn.getResponseCode();
                    if (code < 200 || code >= 300) {
                        throw new IOException("Elasticsearch returned HTTP " + code);
                    }
                } catch (IOException e) {
                    throw new IOException("Failed to connect to Elasticsearch at " + logDestination, e);
                }
            }

            if (!loggingConfigured) {
                ConfigurationBuilder<?> builder = ConfigurationBuilderFactory.newConfigurationBuilder();
                builder.setStatusLevel(Level.ERROR);
                builder.setConfigurationName("WorkerLogConfig");
                builder.add(builder.newRootLogger(Level.DEBUG));
                loggingConfigured = true;
                loggerContext = (LoggerContext) LogManager.getContext(false);
                loggerContext.start(builder.build());
            }

            if (logToEs) {
                URL elasticUrl = new URI(logDestination).toURL();
                Appender elasticAppender = HttpAppender.newBuilder()
                        .setName("ElasticHttpAppender-" + sanitizedWorkerId)
                        .setConfiguration(loggerContext.getConfiguration())
                        .setUrl(elasticUrl)
                        .setLayout(JsonLayout.createDefaultLayout())
                        .build();
                elasticAppender.start();
                loggerContext.getConfiguration().addAppender(elasticAppender);
                loggerContext.getConfiguration().getRootLogger().addAppender(elasticAppender, Level.INFO, null);
            } else {
                Path logDir = Path.of(logDestination);
                if (logDir.toString().isEmpty()) {
                    throw new IllegalArgumentException("Log path is not set in configuration.");
                }

                String logFileName = String.format("logs-worker-%s.log", sanitizedWorkerId);
                Path logFile = logDir.resolve(logFileName);

                Appender fileAppender = FileAppender.newBuilder()
                        .setName("FileAppender-" + sanitizedWorkerId)
                        .withFileName(logFile.toString())
                        .setLayout(PatternLayout.newBuilder()
                                .withPattern("%d [%t] %-5level: %msg%n%throwable")
                                .build())
                        .build();
                fileAppender.start();
                loggerContext.getConfiguration().addAppender(fileAppender);
                loggerContext.getConfiguration().getRootLogger().addAppender(fileAppender, Level.DEBUG, null);

                Appender consoleAppender = ConsoleAppender.newBuilder()
                        .setName("ConsoleAppender")
                        .setLayout(PatternLayout.newBuilder()
                                .withPattern("%d [%t] %-5level: %msg%n%throwable")
                                .build())
                        .build();
                consoleAppender.start();
                loggerContext.getConfiguration().addAppender(consoleAppender);
                loggerContext.getConfiguration().getRootLogger().addAppender(consoleAppender, Level.INFO, null);
            }

            loggerContext.updateLoggers();
            LOGGER = LogManager.getLogger("worker-" + sanitizedWorkerId);
        }
    }


    private void registerWorkerWithCoordinator() {
        if (coordinatorV2Gateway == null) {
            return;
        }
        LOGGER.debug("Registering worker on base URL: {}.", workerBaseUrl);
        int attempts = 5;
        for (int i = 1; i <= attempts; i++) {
            try {
                coordinatorV2Gateway.registerWorker(workerBaseUrl);
                LOGGER.info("Worker {} successfully registered with coordinator.", workerBaseUrl);
                return;
            } catch (Exception e) {
                LOGGER.warn("Attempt {}/{}: Failed to register worker {} with coordinator: {}",
                        i, attempts, workerBaseUrl, e.getMessage());
                try {
                    Thread.sleep(2000L);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
        LOGGER.error("All attempts to register worker {} have failed.", workerBaseUrl);
        throw new RuntimeException();
    }

    private synchronized void waitForTask() throws InterruptedException {
        while (currentTask == null) {
            wait();
        }
    }

    private void executeCurrentTask() {
        try (StorageProvider storageProvider = StorageProviderFactory.getStorageProvider(
                currentTask.taskInformation.storageConnectionString())) {
            try (TemporaryDirectory tempDir = new TemporaryDirectory(storageProvider)) {
                List<Path> localInputFiles = tempDir.get(currentTask.taskInformation.inputFiles(), "input");
                Path outputDir = tempDir.getPath().resolve("output");
                Files.createDirectories(outputDir);
                if (currentTask.taskInformation.taskType().equals(TaskType.MAP)) {
                    MapReduceTasksRunner.executeMapperTask(
                            localInputFiles,
                            currentTask.taskInformation.taskId(),
                            outputDir,
                            currentTask.jobConfiguration,
                            currentTask.job,
                            LOGGER);
                } else if (currentTask.taskInformation.taskType().equals(TaskType.REDUCE)) {
                    MapReduceTasksRunner.executeReduceTask(
                            localInputFiles,
                            currentTask.taskInformation.taskId() - currentTask.jobConfiguration.get(ConfigurationOption.MAPPERS_COUNT),
                            outputDir,
                            currentTask.jobConfiguration,
                            currentTask.job,
                            LOGGER);
                }
                tempDir.put(outputDir, currentTask.taskInformation.targetDir());
                LOGGER.info("Worker {} finished executing task: {}.",
                        workerBaseUrl, currentTask.taskInformation.taskId());
                currentTask.setStatus(Task.TaskStatus.SUCCEED);
            }
        } catch (Exception e) {
            LOGGER.error("Task ID: {} failed with exception.", currentTask.taskInformation.taskId(), e);
            currentTask.setStatus(Task.TaskStatus.FAILED);
            currentTask.status.setException(e);
        }
    }

    private void notifyTaskCompletion(TaskDetails details) {
        if (coordinatorV2Gateway == null) {
            return;
        }
        try {
            coordinatorV2Gateway.notifyTask(details);
            LOGGER.info("Notified coordinator about task completion for Task ID: {}.", details.taskInformation().taskId());
        } catch (IOException | InterruptedException e) {
            LOGGER.error("Failed to notify task completion for Task ID: {}.", details.taskInformation().taskId(), e);
        }
    }

    private synchronized void moveCurrentTaskToHistory() {
        int taskId = currentTask.taskInformation.taskId();
        LOGGER.info("Moving task ID: {} to history.", taskId);
        previousTasks.put(taskId, currentTask);
        currentTask = null;
    }

    private class InMemoryTaskService implements TaskService {
        @Override
        public TaskDetails getTaskDetails(int taskId) {
            return getTaskDetailsInternal(taskId);
        }

        @Override
        public List<TaskStatusInfo> getAllTasks() {
            return getAllTasksInternal();
        }

        @Override
        public TaskDetails createTask(NewTaskDetails details) {
            return createTaskInternal(details);
        }
    }
}
