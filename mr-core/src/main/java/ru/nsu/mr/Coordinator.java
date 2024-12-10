package ru.nsu.mr;

import ru.nsu.mr.config.Configuration;
import ru.nsu.mr.config.ConfigurationOption;
import ru.nsu.mr.endpoints.CoordinatorEndpoint;
import ru.nsu.mr.endpoints.LoggerWithMetricsCalculation;
import ru.nsu.mr.endpoints.MetricsService;
import ru.nsu.mr.endpoints.dto.NewTaskDetails;
import ru.nsu.mr.endpoints.dto.TaskDetails;
import ru.nsu.mr.endpoints.dto.TaskType;
import ru.nsu.mr.manager.WorkerManager;

import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

public class Coordinator {

    private enum Phase {
        MAP,
        REDUCE,
        JOB_ENDED
    }

    private static class ConnectedWorker {
        private final String port;
        private final WorkerManager manager;
        private Integer currentTaskId = null;

        public ConnectedWorker(String port) {
            this.port = port;
            this.manager = new WorkerManager(port);
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

        public synchronized WorkerManager getManager() {
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

    private static final Logger LOGGER = LogManager.getLogger();

    public Coordinator(Configuration config) throws IOException {
        this.configuration = config;
        this.jobLogger = new LoggerWithMetricsCalculation();

        this.mappersCount = configuration.get(ConfigurationOption.MAPPERS_COUNT);
        this.reducersCount = configuration.get(ConfigurationOption.REDUCERS_COUNT);

        endpoint =
                new CoordinatorEndpoint(
                        configuration.get(ConfigurationOption.METRICS_PORT),
                        (MetricsService) jobLogger,
                        this::registerWorker,
                        this::receiveTaskCompletion);
        endpoint.startServer();
    }

    public void start(List<Path> inputFiles) throws InterruptedException {
        waitForWorker();

        LOGGER.debug("Mappers quantity: {}.", mappersCount);
        int numberOfProcessedInputFiles = 0;
        for (int i = 0; i < mappersCount; ++i) {
            int inputFilesToProcessCount =
                    (inputFiles.size() - numberOfProcessedInputFiles) / (mappersCount - i);
            List<String> inputFilesToProcess = new ArrayList<>();
            for (int k = 0; k < inputFilesToProcessCount; ++k) {
                inputFilesToProcess.add(inputFiles.get(numberOfProcessedInputFiles + k).toString());
            }
            mapTaskQueue.add(new NewTaskDetails(i, TaskType.MAP, inputFilesToProcess));
            numberOfProcessedInputFiles += inputFilesToProcessCount;
        }

        LOGGER.debug("Reducers quantity: {}.", reducersCount);
        for (int i = 0; i < reducersCount; ++i) {
            List<String> interFilesToReduce = new ArrayList<>();
            for (int k = 0; k < mappersCount; ++k) {
                interFilesToReduce.add("mapper-output-" + k + "-" + i + ".txt");
            }
            reduceTaskQueue.add(
                    new NewTaskDetails(mappersCount + i, TaskType.REDUCE, interFilesToReduce));
        }
        distributeTasks();
        waitForJobEnd();
        Thread.sleep(100);
        endpoint.stopServer();
    }

    private synchronized void registerWorker(String port) {
        workers.add(new ConnectedWorker(port));
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
                    new NewTaskDetails(details.taskId(), details.taskType(), details.inputFiles()));
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
                LOGGER.debug("Current free worker: {}", worker.hashCode());
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
