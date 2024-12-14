package ru.nsu.mr;

import ru.nsu.mr.config.Configuration;
import ru.nsu.mr.config.ConfigurationOption;
import ru.nsu.mr.endpoints.CoordinatorEndpoint;
import ru.nsu.mr.endpoints.LoggerWithMetricsCalculation;
import ru.nsu.mr.endpoints.MetricsService;
import ru.nsu.mr.endpoints.dto.NewTaskDetails;
import ru.nsu.mr.endpoints.dto.TaskDetails;
import ru.nsu.mr.endpoints.dto.TaskType;
import ru.nsu.mr.gateway.WorkerGateway;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

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

public class Coordinator {

    private enum Phase {
        MAP,
        REDUCE,
        JOB_ENDED
    }

    private static class ConnectedWorker {
        private final String port;
        private final WorkerGateway manager;
        private Integer currentTaskId = null;

        public ConnectedWorker(String port) {
            this.port = port;
            this.manager = new WorkerGateway(port);
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

        public synchronized WorkerGateway getManager() {
            return manager;
        }
    }

    private final List<ConnectedWorker> workers = new ArrayList<>();
    private final Queue<NewTaskDetails> mapTaskQueue = new ConcurrentLinkedQueue<>();
    private final Queue<NewTaskDetails> reduceTaskQueue = new ConcurrentLinkedQueue<>();
    private final Configuration configuration;
    private final JobLogger jobLogger;

    private final int mappersCount;
    private final int reducersCount;
    private int finishedMappersCount = 0;
    private int finishedReducersCount = 0;
    private Phase currentPhase = Phase.MAP;
    private CoordinatorEndpoint endpoint;

    private static Logger LOGGER = null;
    private static LoggerContext context;
    private static final Object lock = new Object();
    private static boolean isConfigured = false;
    private static ConfigurationBuilder<?> builder;

    public Coordinator(Configuration config) throws IOException {
        this.configuration = config;
        this.jobLogger = new LoggerWithMetricsCalculation();

        this.mappersCount = configuration.get(ConfigurationOption.MAPPERS_COUNT);
        this.reducersCount = configuration.get(ConfigurationOption.REDUCERS_COUNT);

        configureLogging();

        endpoint =
                new CoordinatorEndpoint(
                        configuration.get(ConfigurationOption.METRICS_PORT),
                        (MetricsService) jobLogger,
                        this::registerWorker,
                        this::receiveTaskCompletion);
        endpoint.startServer();
    }

    private void configureLogging() throws IOException {
        synchronized (lock) {
            Path logPath = Path.of(configuration.get(ConfigurationOption.LOGS_PATH));
            String port = configuration.get(ConfigurationOption.METRICS_PORT);

            if (logPath.toString().isEmpty()) {
                throw new IllegalArgumentException("Log path is not set in configuration.");
            }
            Files.createDirectories(logPath);
            String logFileName = String.format("logs-coordinator-%s.log", port);
            Path logFile = logPath.resolve(logFileName);

            if (!isConfigured) {
                builder = ConfigurationBuilderFactory.newConfigurationBuilder();
                builder.setStatusLevel(Level.ERROR);
                builder.setConfigurationName("LogConfig");

                builder.add(builder.newRootLogger(Level.DEBUG));
                isConfigured = true;

                context = (LoggerContext) LogManager.getContext(false);
                context.start(builder.build());
            }

            // Appender for a file
            String appenderName = "FileAppender-" + port;
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
            LOGGER = LogManager.getLogger("coordinator-" + port);
        }
    }

    public void start(List<Path> inputFiles) throws InterruptedException {

        LOGGER.debug("Mappers quantity: {}.", mappersCount);

        int numberOfProcessedInputFiles = 0;
        for (int i = 0; i < mappersCount; ++i) {
            int inputFilesToProcessCount =
                    (inputFiles.size() - numberOfProcessedInputFiles) / (mappersCount - i);
            List<String> inputFilesToProcess = new ArrayList<>();
            for (int k = 0; k < inputFilesToProcessCount; ++k) {
                inputFilesToProcess.add(inputFiles.get(numberOfProcessedInputFiles + k).toString());
            }
            mapTaskQueue.add(new NewTaskDetails(i, TaskType.MAP, inputFilesToProcess, null));
            numberOfProcessedInputFiles += inputFilesToProcessCount;
        }

        LOGGER.debug("Reducers quantity: {}.", reducersCount);
        for (int i = 0; i < reducersCount; ++i) {
            List<String> interFilesToReduce = new ArrayList<>();
            for (int k = 0; k < mappersCount; ++k) {
                interFilesToReduce.add("mapper-output-" + k + "-" + i + ".txt");
            }
            reduceTaskQueue.add(
                    new NewTaskDetails(
                            mappersCount + i, TaskType.REDUCE, interFilesToReduce, null));
        }

        waitForWorker();
        waitForJobEnd();
        Thread.sleep(100);
        endpoint.stopServer();
    }

    private synchronized void registerWorker(String port) {
        workers.add(new ConnectedWorker(port));
        distributeTasks();
        notifyAll();
    }

    private synchronized void receiveTaskCompletion(TaskDetails details) {
        if ("SUCCEED".equals(details.status())) {
            if (details.taskType() == TaskType.MAP) {
                finishedMappersCount++;
                if (finishedMappersCount == mappersCount) {
                    currentPhase = Phase.REDUCE;
                    LOGGER.info("All MAP tasks have been completed. Transitioning to REDUCE phase.");
                }
            } else {
                finishedReducersCount++;
                if (finishedReducersCount == reducersCount) {
                    currentPhase = Phase.JOB_ENDED;
                    LOGGER.info("Job has ended.");
                    notifyAll();
                }
            }
        } else {
            Queue<NewTaskDetails> targetQueue =
                    details.taskType() == TaskType.MAP ? mapTaskQueue : reduceTaskQueue;
            targetQueue.add(
                    new NewTaskDetails(
                            details.taskId(), details.taskType(), details.inputFiles(), null));
            return;
        }

        workers.stream()
                .filter(w -> Objects.equals(w.currentTaskId, details.taskId()))
                .findFirst()
                .ifPresent(ConnectedWorker::release);

        LOGGER.info("Task: {} has been completed.", details.taskId());

        distributeTasks();
    }

    private synchronized void distributeTasks() {
        LOGGER.info("Coordinator started distributing tasks.");

        Queue<NewTaskDetails> currentTaskQueue =
                currentPhase == Phase.MAP ? mapTaskQueue : reduceTaskQueue;

        while (!currentTaskQueue.isEmpty()) {
            Optional<ConnectedWorker> freeWorker =
                    workers.stream().filter(ConnectedWorker::isFree).findFirst();

            if (freeWorker.isPresent()) {
                NewTaskDetails task = currentTaskQueue.poll();
                ConnectedWorker worker = freeWorker.get();
                LOGGER.debug("Current free worker on port: {}", worker.port);
                try {
                    worker.getManager().createTask(task);
                    worker.assignTask(task.taskId());
                    LOGGER.info("Coordinator assigned task: {} to the worker on port: {}",
                            task.taskId(), worker.port);
                } catch (IOException | InterruptedException e) {
                    LOGGER.error("Coordinator failed to assign task: {} to the worker on port: {}",
                            task.taskId(), worker.port);
                    currentTaskQueue.add(task);
                    break;
                }
            } else {
                break;
            }
        }
        LOGGER.info("Coordinator finished distributing tasks.");
    }

    private synchronized void waitForWorker() throws InterruptedException {
        while (workers.isEmpty()) {
            wait();
        }
    }

    private synchronized void waitForJobEnd() throws InterruptedException {
        while (currentPhase != Phase.JOB_ENDED) {
            wait();
        }
    }
}
