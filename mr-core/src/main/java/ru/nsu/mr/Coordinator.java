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
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

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
    private Phase currentPhase = Phase.MAP;
    private int finishedMappersCount = 0;
    private int finishedReducersCount = 0;
    private static final long HEARTBEAT_PERIOD_MS = 5000;
    private static final long HEARTBEAT_TIMEOUT_MS = 2000;
    private final ScheduledExecutorService heartbeatScheduler = Executors.newSingleThreadScheduledExecutor();

    private static class ConnectedWorker {
        private final String workerBaseUrl;
        private final WorkerGateway gateway;
        private Integer currentTaskId = null;
        private NewTaskDetails currentTaskDetails = null;

        public ConnectedWorker(String workerBaseUrl) {
            this.workerBaseUrl = workerBaseUrl;
            this.gateway = new WorkerGateway(workerBaseUrl);
        }

        public synchronized boolean isFree() {
            return currentTaskId == null;
        }

        public synchronized void assignTask(NewTaskDetails task) {
            this.currentTaskId = task.taskInformation().taskId();
            this.currentTaskDetails = task;
        }

        public synchronized void release() {
            this.currentTaskId = null;
            this.currentTaskDetails = null;
        }

        public synchronized NewTaskDetails getCurrentTaskDetails() {
            return currentTaskDetails;
        }

        public synchronized WorkerGateway getGateway() {
            return gateway;
        }
    }

    public Coordinator(String coordinatorBaseUrl) throws IOException {
        this.coordinatorBaseUrl = coordinatorBaseUrl;
        configureLogging();
        endpoint = new CoordinatorEndpoint(coordinatorBaseUrl, this::registerWorker, this::receiveTaskCompletion);
        endpoint.startServer();

        heartbeatScheduler.scheduleAtFixedRate(
                this::checkAllWorkersHealth,
                HEARTBEAT_PERIOD_MS,
                HEARTBEAT_PERIOD_MS,
                TimeUnit.MILLISECONDS
        );
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

    public void start() throws InterruptedException {
        if (jobConfig == null) {
            throw new IllegalStateException("Job configuration not set.");
        }
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
            LOGGER.info("Found {} input files in {}", inputFiles.size(), inputsPath);
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
            LOGGER.error("Error while creating MAP tasks", e);
            throw new RuntimeException("Failed to create MAP tasks", e);
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
        waitForJobEnd();
        Thread.sleep(1000);
        heartbeatScheduler.shutdownNow();
        endpoint.stopServer();
        LOGGER.info("Job has finished successfully.");
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
        if (currentPhase == Phase.MAP) {
            assignTasksFromQueue(mapTaskQueue);
        } else if (currentPhase == Phase.REDUCE) {
            assignTasksFromQueue(reduceTaskQueue);
        }
    }

    private void assignTasksFromQueue(Queue<NewTaskDetails> queue) {
        while (!queue.isEmpty()) {
            Optional<ConnectedWorker> freeWorker =
                    workers.stream().filter(ConnectedWorker::isFree).findFirst();
            if (freeWorker.isPresent()) {
                NewTaskDetails task = queue.poll();
                ConnectedWorker worker = freeWorker.get();
                try {
                    worker.getGateway().createTask(task);
                    worker.assignTask(task);
                    LOGGER.info("Assigned task {} to worker {}",
                            task.taskInformation().taskId(), worker.workerBaseUrl);
                } catch (IOException | InterruptedException e) {
                    LOGGER.error("Failed to assign task {} to worker {}",
                            task.taskInformation().taskId(),
                            worker.workerBaseUrl,
                            e);
                    queue.add(task);
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

    private void checkAllWorkersHealth() {
        List<ConnectedWorker> currentWorkers;
        synchronized (this) {
            currentWorkers = new ArrayList<>(workers);
        }

        for (ConnectedWorker w : currentWorkers) {
            try {
                if (!isWorkerAlive(w)) {
                    LOGGER.warn("Worker {} is considered DEAD. Reassigning task.",
                            w.workerBaseUrl);
                    handleDeadWorker(w);
                }
            } catch (Exception e) {
                LOGGER.error("Error in heartbeat check for worker {}",
                        w.workerBaseUrl, e);
            }
        }
    }

    private boolean isWorkerAlive(ConnectedWorker worker) {
        return worker.getGateway().isAlive();
    }

    private synchronized void handleDeadWorker(ConnectedWorker worker) {
        workers.remove(worker);
        NewTaskDetails assignedTask = worker.getCurrentTaskDetails();
        if (assignedTask != null) {
            if (assignedTask.taskInformation().taskType() == TaskType.MAP) {
                mapTaskQueue.add(assignedTask);
            } else {
                reduceTaskQueue.add(assignedTask);
            }
            worker.release();
        }
        distributeTasks();
    }

    private void configureLogging() throws IOException, URISyntaxException {
        synchronized (lock) {
            String url = coordinatorBaseUrl;
            Path logPath = Path.of("./logs");
            if (Files.exists(logPath)) {
                deleteDirectory(logPath);
            }
            Files.createDirectories(logPath);
            String logFileName = String.format("logs-coordinator-%s.log", (new URI(url)).getPort());
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
            String appenderName = "FileAppender-" + url;
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
            LOGGER = LogManager.getLogger("coordinator-" + url);
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
