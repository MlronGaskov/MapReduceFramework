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
    private final Logger logger;

    private final int mappersCount;
    private final int reducersCount;
    private int finishedMappersCount = 0;
    private int finishedReducersCount = 0;
    private Phase currentPhase = Phase.MAP;
    private CoordinatorEndpoint endpoint;

    public Coordinator(Configuration config) throws IOException {
        this.configuration = config;
        this.logger = new LoggerWithMetricsCalculation();

        this.mappersCount = configuration.get(ConfigurationOption.MAPPERS_COUNT);
        this.reducersCount = configuration.get(ConfigurationOption.REDUCERS_COUNT);

        endpoint =
                new CoordinatorEndpoint(
                        configuration.get(ConfigurationOption.METRICS_PORT),
                        (MetricsService) logger,
                        this::registerWorker,
                        this::receiveTaskCompletion);
        endpoint.startServer();
    }

    public void start(List<Path> inputFiles) throws InterruptedException {
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
                    System.out.println("All MAP tasks completed. Transitioning to REDUCE phase.");
                }
            } else {
                finishedReducersCount++;
                if (finishedReducersCount == reducersCount) {
                    currentPhase = Phase.JOB_ENDED;
                    System.out.println("Job ended.");
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

        System.out.println("Task " + details.taskId() + " completed.");

        distributeTasks();
    }

    private synchronized void distributeTasks() {
        Queue<NewTaskDetails> currentTaskQueue =
                currentPhase == Phase.MAP ? mapTaskQueue : reduceTaskQueue;

        while (!currentTaskQueue.isEmpty()) {
            Optional<ConnectedWorker> freeWorker =
                    workers.stream().filter(ConnectedWorker::isFree).findFirst();

            if (freeWorker.isPresent()) {
                NewTaskDetails task = currentTaskQueue.poll();
                ConnectedWorker worker = freeWorker.get();
                try {
                    worker.getManager().createTask(task);
                    worker.assignTask(task.taskId());
                    System.out.println(
                            "Assigned task "
                                    + task.taskId()
                                    + " to worker on port: "
                                    + worker.port);
                } catch (IOException | InterruptedException e) {
                    System.err.println(
                            "Failed to assign task "
                                    + task.taskId()
                                    + " to worker on port: "
                                    + worker.port);
                    currentTaskQueue.add(task);
                    break;
                }
            } else {
                break;
            }
        }
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
