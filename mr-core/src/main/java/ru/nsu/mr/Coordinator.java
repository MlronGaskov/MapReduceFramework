package ru.nsu.mr;

import ru.nsu.mr.config.Configuration;
import ru.nsu.mr.config.ConfigurationOption;
import ru.nsu.mr.endpoints.CoordinatorEndpoint;
import ru.nsu.mr.endpoints.dto.JobInformation;
import ru.nsu.mr.endpoints.dto.NewTaskDetails;
import ru.nsu.mr.endpoints.dto.TaskDetails;
import ru.nsu.mr.endpoints.dto.TaskInformation;
import ru.nsu.mr.endpoints.dto.TaskType;
import ru.nsu.mr.gateway.WorkerGateway;
import ru.nsu.mr.storages.StorageProvider;
import ru.nsu.mr.storages.StorageProviderFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.builder.api.ConfigurationBuilder;
import org.apache.logging.log4j.core.config.builder.api.ConfigurationBuilderFactory;
import org.apache.logging.log4j.core.appender.ConsoleAppender;
import org.apache.logging.log4j.core.appender.FileAppender;
import org.apache.logging.log4j.core.layout.PatternLayout;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

public class Coordinator {

    private enum Phase {
        MAP,
        REDUCE,
        JOB_ENDED
    }

    private static Logger LOGGER = null;
    private static LoggerContext context;
    private static final Object lock = new Object();
    private static boolean isConfigured = false;

    private final String coordinatorBaseUrl;
    private final CoordinatorEndpoint endpoint;
    private final List<ConnectedWorker> workers = new ArrayList<>();
    private final Queue<NewTaskDetails> mapTaskQueue = new ConcurrentLinkedQueue<>();
    private final Queue<NewTaskDetails> reduceTaskQueue = new ConcurrentLinkedQueue<>();
    private Configuration jobConfig;
    private Phase currentPhase;
    private int finishedMappersCount;
    private int finishedReducersCount;

    private final Object jobLock = new Object();
    private volatile boolean busy = false;
    private volatile Configuration pendingJob = null;

    private static class ConnectedWorker {
        private final String workerBaseUrl;
        private final WorkerGateway gateway;
        private Integer currentTaskId = null;

        public ConnectedWorker(String workerBaseUrl) {
            this.workerBaseUrl = workerBaseUrl;
            this.gateway = new WorkerGateway(workerBaseUrl);
        }

        public synchronized boolean isFree() {
            return currentTaskId == null;
        }

        public synchronized void assignTask(int taskId) {
            this.currentTaskId = taskId;
        }

        public synchronized void release() {
            this.currentTaskId = null;
        }

        public synchronized WorkerGateway getGateway() {
            return gateway;
        }
    }

    public Coordinator(String coordinatorBaseUrl) throws IOException {
        this.coordinatorBaseUrl = coordinatorBaseUrl;
        configureLogging();
        endpoint = new CoordinatorEndpoint(
                coordinatorBaseUrl,
                this::registerWorker,
                this::receiveTaskCompletion,
                this::submitJob);
        endpoint.startServer();
    }

    public void setJobConfiguration(Configuration jobConfiguration) {
        if (this.jobConfig != null) {
            throw new IllegalStateException("Job configuration already set.");
        }
        this.jobConfig = jobConfiguration;
    }

    public void setJobConfiguration(String yamlFilePath) throws IOException {
        ConfigurationLoader loader = new ConfigurationLoader(yamlFilePath);
        Configuration config = loader.getConfig();
        setJobConfiguration(config);
    }

    public void submitJob(Configuration job) {
        if (busy) {
            throw new IllegalStateException("Coordinator is busy");
        }
        synchronized (jobLock) {
            pendingJob = job;
            busy = true;
            jobLock.notifyAll();
        }
    }

    private void executeJob() throws InterruptedException {
        currentPhase = Phase.MAP;
        finishedMappersCount = 0;
        finishedReducersCount = 0;
        mapTaskQueue.clear();
        reduceTaskQueue.clear();

        LOGGER.info("Starting job with configuration: JOB_PATH={}, MAPPERS_COUNT={}, REDUCERS_COUNT={}",
                jobConfig.get(ConfigurationOption.JOB_PATH),
                jobConfig.get(ConfigurationOption.MAPPERS_COUNT),
                jobConfig.get(ConfigurationOption.REDUCERS_COUNT));

        String jobPath = jobConfig.get(ConfigurationOption.JOB_PATH);
        String jobStorageConnectionString = jobConfig.get(ConfigurationOption.JOB_STORAGE_CONNECTION_STRING);
        String dataStorageConnectionString = jobConfig.get(ConfigurationOption.DATA_STORAGE_CONNECTION_STRING);
        String inputsPath = jobConfig.get(ConfigurationOption.INPUTS_PATH);
        String mappersOutputsPath = jobConfig.get(ConfigurationOption.MAPPERS_OUTPUTS_PATH);
        String reducersOutputsPath = jobConfig.get(ConfigurationOption.REDUCERS_OUTPUTS_PATH);
        int mappersCount = jobConfig.get(ConfigurationOption.MAPPERS_COUNT);
        int reducersCount = jobConfig.get(ConfigurationOption.REDUCERS_COUNT);
        int sorterInMemoryRecords = jobConfig.get(ConfigurationOption.SORTER_IN_MEMORY_RECORDS);
        JobInformation jobInformation = new JobInformation(
                1,
                jobPath,
                jobStorageConnectionString,
                mappersCount,
                reducersCount,
                sorterInMemoryRecords
        );
        try (StorageProvider storageProvider = StorageProviderFactory.getStorageProvider(dataStorageConnectionString)) {
            List<String> inputFiles = storageProvider.list(inputsPath);
            int totalInputs = inputFiles.size();
            LOGGER.info("Found {} input files in {}\n {}", inputFiles.size(), inputsPath, inputFiles);
            int processed = 0;
            for (int i = 0; i < mappersCount; i++) {
                int count = (totalInputs - processed) / (mappersCount - i);
                List<String> filesForTask = new ArrayList<>();
                for (int j = 0; j < count; j++) {
                    filesForTask.add(inputFiles.get(processed + j));
                }
                processed += count;
                TaskInformation taskInfo = new TaskInformation(
                        i,
                        TaskType.MAP,
                        filesForTask,
                        mappersOutputsPath,
                        dataStorageConnectionString
                );
                LOGGER.info("Created MAP task {} with {} files", i, filesForTask.size());
                NewTaskDetails newTask = new NewTaskDetails(jobInformation, taskInfo);
                mapTaskQueue.add(newTask);
            }
        } catch (Exception e) {
            LOGGER.error("Error while creating MAP tasks: {}", e.getMessage());
            throw new RuntimeException(e);
        }
        for (int i = 0; i < reducersCount; i++) {
            List<String> reduceInputs = new ArrayList<>();
            for (int j = 0; j < mappersCount; j++) {
                reduceInputs.add(mappersOutputsPath + "/mapper-output-" + j + "-" + i + ".txt");
            }
            TaskInformation taskInfo = new TaskInformation(
                    mappersCount + i,
                    TaskType.REDUCE,
                    reduceInputs,
                    reducersOutputsPath,
                    dataStorageConnectionString
            );
            NewTaskDetails newTask = new NewTaskDetails(jobInformation, taskInfo);
            reduceTaskQueue.add(newTask);
            LOGGER.info("Created REDUCE task {} with {} input files", mappersCount + i, reduceInputs.size());
        }
        distributeTasks();
        waitForJobEnd();
        Thread.sleep(1000);
        LOGGER.info("Job has finished successfully.");
    }


    public void start() throws InterruptedException {
        if (jobConfig != null) {
            executeJob();
            Thread.sleep(1000);
            endpoint.stopServer();
        } else {
            LOGGER.info("Starting job without initial configuration.");
            while (!Thread.currentThread().isInterrupted()) {
                synchronized (jobLock) {
                    LOGGER.info("Waiting for job.");
                    while (pendingJob == null) {
                        jobLock.wait();
                    }
                    this.jobConfig = pendingJob;
                    pendingJob = null;
                }
                try {
                    executeJob();
                } catch (Exception e) {
                    LOGGER.error("Job execution failed: {}", e.getMessage());
                } finally {
                    synchronized (jobLock) {
                        busy = false;
                        jobConfig = null;
                    }
                }
            }
        }
    }

    private synchronized void registerWorker(String workerBaseUrl) {
        ConnectedWorker worker = new ConnectedWorker(workerBaseUrl);
        workers.add(worker);
        distributeTasks();
        notifyAll();
    }

    private synchronized void receiveTaskCompletion(TaskDetails details) {
        if ("SUCCEED".equals(details.status())) {
            if (details.taskInformation().taskType() == TaskType.MAP) {
                finishedMappersCount++;
                if (finishedMappersCount == jobConfig.get(ConfigurationOption.MAPPERS_COUNT)) {
                    currentPhase = Phase.REDUCE;
                    LOGGER.info("All MAP tasks completed. Transitioning to REDUCE phase.");
                }
            } else {
                finishedReducersCount++;
                if (finishedReducersCount == jobConfig.get(ConfigurationOption.REDUCERS_COUNT)) {
                    currentPhase = Phase.JOB_ENDED;
                    LOGGER.info("All REDUCE tasks completed. Job ended.");
                    notifyAll();
                }
            }
        } else {
            NewTaskDetails failedTask = new NewTaskDetails(
                    new JobInformation(
                            1,
                            jobConfig.get(ConfigurationOption.JOB_PATH),
                            jobConfig.get(ConfigurationOption.JOB_STORAGE_CONNECTION_STRING),
                            jobConfig.get(ConfigurationOption.MAPPERS_COUNT),
                            jobConfig.get(ConfigurationOption.REDUCERS_COUNT),
                            jobConfig.get(ConfigurationOption.SORTER_IN_MEMORY_RECORDS)
                    ),
                    details.taskInformation()
            );
            if (details.taskInformation().taskType() == TaskType.MAP) {
                mapTaskQueue.add(failedTask);
            } else {
                reduceTaskQueue.add(failedTask);
            }
        }
        workers.stream()
                .filter(w -> Objects.equals(w.currentTaskId, details.taskInformation().taskId()))
                .findFirst()
                .ifPresent(ConnectedWorker::release);
        distributeTasks();
    }

    private synchronized void distributeTasks() {
        Queue<NewTaskDetails> currentQueue = currentPhase == Phase.MAP ? mapTaskQueue : reduceTaskQueue;
        while (!currentQueue.isEmpty()) {
            Optional<ConnectedWorker> freeWorker = workers.stream().filter(ConnectedWorker::isFree).findFirst();
            if (freeWorker.isPresent()) {
                NewTaskDetails task = currentQueue.poll();
                ConnectedWorker worker = freeWorker.get();
                try {
                    worker.getGateway().createTask(task);
                    worker.assignTask(task.taskInformation().taskId());
                    LOGGER.info("Assigned task {} to worker {}", task.taskInformation().taskId(), worker.workerBaseUrl);
                } catch (IOException | InterruptedException e) {
                    LOGGER.error("Failed to assign task {} to worker {}: {}",
                            task.taskInformation().taskId(), worker.workerBaseUrl, e.getMessage());
                    currentQueue.add(task);
                    break;
                }
            } else {
                break;
            }
        }
    }

    private synchronized void waitForJobEnd() throws InterruptedException {
        while (currentPhase != Phase.JOB_ENDED) {
            wait();
        }
    }

    private void configureLogging() throws IOException {
        synchronized (lock) {
            String sanitizedCoordinatorId = coordinatorBaseUrl
                    .replaceAll("https?://", "")
                    .replaceAll("[^a-zA-Z0-9.-]", "_");

            Path logPath = Path.of("./logs");
            if (Files.exists(logPath)) {
                deleteDirectory(logPath);
            }
            Files.createDirectories(logPath);

            String logFileName = String.format("logs-coordinator-%s.log", sanitizedCoordinatorId);
            Path logFile = logPath.resolve(logFileName);

            if (!isConfigured) {
                ConfigurationBuilder<?> builder = ConfigurationBuilderFactory.newConfigurationBuilder();
                builder.setStatusLevel(Level.ERROR);
                builder.setConfigurationName("LogConfig");
                builder.add(builder.newRootLogger(Level.DEBUG));
                isConfigured = true;
                context = (LoggerContext) LogManager.getContext(false);
                context.start(builder.build());
            }

            String appenderName = "FileAppender-" + sanitizedCoordinatorId;
            FileAppender fileAppender = FileAppender.newBuilder()
                    .setName(appenderName)
                    .withFileName(logFile.toString())
                    .setLayout(PatternLayout.newBuilder()
                            .withPattern("%d [%t] %-5level: %msg%n%throwable")
                            .build())
                    .build();
            fileAppender.start();
            context.getConfiguration().addAppender(fileAppender);
            context.getConfiguration().getRootLogger().addAppender(fileAppender, Level.DEBUG, null);

            String consoleAppenderName = "ConsoleAppender";
            ConsoleAppender consoleAppender = ConsoleAppender.newBuilder()
                    .setName(consoleAppenderName)
                    .setLayout(PatternLayout.newBuilder()
                            .withPattern("%d [%t] %-5level: %msg%n%throwable")
                            .build())
                    .build();
            consoleAppender.start();
            context.getConfiguration().addAppender(consoleAppender);
            context.getConfiguration().getRootLogger().addAppender(consoleAppender, Level.INFO, null);

            context.updateLoggers();
            LOGGER = LogManager.getLogger("coordinator-" + sanitizedCoordinatorId);
        }
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
