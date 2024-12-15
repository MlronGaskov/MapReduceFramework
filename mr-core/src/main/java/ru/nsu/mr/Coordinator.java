package ru.nsu.mr;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.SimpleLayout;

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
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

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

    // Создаём логгер
    private static final Logger LOG = Logger.getLogger(Coordinator.class);

    public Coordinator(Configuration config) throws IOException {
        this.configuration = config;
        this.jobLogger = new LoggerWithMetricsCalculation();

        this.mappersCount = configuration.get(ConfigurationOption.MAPPERS_COUNT);
        this.reducersCount = configuration.get(ConfigurationOption.REDUCERS_COUNT);

        // Инициализация логирования
        initLogging();

        LOG.info("Coordinator is starting...");

        endpoint =
                new CoordinatorEndpoint(
                        configuration.get(ConfigurationOption.METRICS_PORT),
                        (MetricsService) jobLogger,
                        this::registerWorker,
                        this::receiveTaskCompletion);
        endpoint.startServer();
    }

    private void initLogging() throws IOException {
        String logsPath = configuration.get(ConfigurationOption.LOGS_PATH);
        String metricsPort = configuration.get(ConfigurationOption.METRICS_PORT);
        String logFileName = logsPath + "/coordinator" + metricsPort + ".log";

        // Настраиваем простой формат для логов
        SimpleLayout layout = new SimpleLayout();

        // Файловый аппендер с дозаписью
        FileAppender fileAppender = new FileAppender(layout, logFileName, true);
        // Консольный аппендер
        ConsoleAppender consoleAppender = new ConsoleAppender(layout);

        // Настраиваем корневой логгер
        Logger rootLogger = Logger.getRootLogger();
        rootLogger.setLevel(Level.DEBUG); // Можно настроить уровень логирования
        rootLogger.addAppender(fileAppender);
        rootLogger.addAppender(consoleAppender);
    }

    public void start(List<Path> inputFiles) throws InterruptedException {
        LOG.info("Starting job...");

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
        LOG.info("Job ended.");
    }

    private synchronized void registerWorker(String port) {
        LOG.info("Registering worker at port: " + port);
        workers.add(new ConnectedWorker(port));
        distributeTasks();
        notifyAll();
    }

    private synchronized void receiveTaskCompletion(TaskDetails details) {
        LOG.info(
                "Received task completion: "
                        + details.taskId()
                        + " with status: "
                        + details.status());
        if ("SUCCEED".equals(details.status())) {
            if (details.taskType() == TaskType.MAP) {
                finishedMappersCount++;
                if (finishedMappersCount == mappersCount) {
                    currentPhase = Phase.REDUCE;
                    LOG.info("All map tasks finished. Moving to reduce phase.");
                }
            } else {
                finishedReducersCount++;
                if (finishedReducersCount == reducersCount) {
                    currentPhase = Phase.JOB_ENDED;
                    LOG.info("All reduce tasks finished. Job ended.");
                    notifyAll();
                }
            }
        } else {
            LOG.warn("Task " + details.taskId() + " failed. Re-queueing task.");
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

        distributeTasks();
    }

    private synchronized void distributeTasks() {
        LOG.debug("Distributing tasks...");
        Queue<NewTaskDetails> currentTaskQueue =
                currentPhase == Phase.MAP ? mapTaskQueue : reduceTaskQueue;

        while (!currentTaskQueue.isEmpty()) {
            Optional<ConnectedWorker> freeWorker =
                    workers.stream().filter(ConnectedWorker::isFree).findFirst();

            if (freeWorker.isPresent()) {
                NewTaskDetails task = currentTaskQueue.poll();
                ConnectedWorker worker = freeWorker.get();
                try {
                    LOG.info(
                            "Assigning task "
                                    + task.taskId()
                                    + " to worker on port "
                                    + worker.port);
                    worker.getManager().createTask(task);
                    worker.assignTask(task.taskId());
                } catch (IOException | InterruptedException e) {
                    LOG.error(
                            "Error while assigning task " + task.taskId() + ": " + e.getMessage());
                    currentTaskQueue.add(task);
                    break;
                }
            } else {
                LOG.debug("No free workers available at the moment.");
                break;
            }
        }
    }

    private synchronized void waitForWorker() throws InterruptedException {
        LOG.debug("Waiting for at least one worker to register...");
        while (workers.isEmpty()) {
            wait();
        }
        LOG.debug("At least one worker registered, continuing.");
    }

    private synchronized void waitForJobEnd() throws InterruptedException {
        LOG.debug("Waiting for job to end...");
        while (currentPhase != Phase.JOB_ENDED) {
            wait();
        }
        LOG.debug("Job ended, proceeding.");
    }
}
